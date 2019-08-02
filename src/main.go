/**
 * Using Aliyun's Archive Storage (OSS) to backup all files in a directory
 * This is a good way to be used along with Private Cloud Storage
 * To get a cheap and reliable storage
 */
package main

import (
	"bufio"
	"compress/flate"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/karrick/godirwalk"
	_ "github.com/mattn/go-sqlite3"
	"github.com/panjf2000/ants"
	"gopkg.in/djherbis/times.v1"
)

const version string = "v0.1"

var fileCounter int
var onlineChunksSet map[string]bool
var logLevel int8 = 1 // 0: verbose 1:info 2: none
var cacheDB *sql.DB
var sizeToUpload int64

type fileInfo struct {
	Path         string
	ChunkKey     string
	Size         int64
	ModTime      int64
	CreationTime int64
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func initCache(basepath string) {
	// 打开数据库，如果不存在，则创建
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(basepath, ".__ossIndex_special_.cache.dat?cache=shared"))
	cacheDB = db
	checkErr(err)
	db.SetMaxOpenConns(1)

	// 创建表（如果已经创建则忽略错误）
	sqlTable := `
	CREATE TABLE IF NOT EXISTS index_cache(
		path TEXT NOT NULL,
		modTime BIGINT NOT NULL,
		size BIGINT NOT NULL,
		sha512 TEXT NOT NULL,
		lastSeenTime BIGINT NOT NULL
	);

	CREATE UNIQUE INDEX IF NOT EXISTS index_key_value 
	on index_cache (path, modTime, size);
	`

	_, err = cacheDB.Exec(sqlTable)
	checkErr(err)
}

/*
 * generate hash information of a file
 * if fastMode is true, sha512 cache will be used according to file last-modified-time and file path.
 */
func getFileHashInfo(file string, relativePath string, fastMode bool, tx *sql.Tx) (fileInfo, bool, error) {
	stat, err := os.Stat(file)
	if err != nil {
		return fileInfo{}, false, err
	}

	fileTime := times.Get(stat)
	resInfo := fileInfo{
		Path:         relativePath,
		Size:         stat.Size(),
		ModTime:      stat.ModTime().UnixNano(),
		CreationTime: fileTime.BirthTime().UnixNano(),
	}

	if fastMode {
		var shaVal string

		row := tx.QueryRow("SELECT sha512 FROM index_cache WHERE path = ? AND modTime = ? AND size = ?", relativePath, resInfo.ModTime, resInfo.Size)

		if row != nil && row.Scan(&shaVal) == nil {
			resInfo.ChunkKey = shaVal

			_, err = tx.Exec("UPDATE index_cache SET lastSeenTime = ? WHERE path = ? AND modTime = ? AND size = ?", time.Now().UnixNano(), relativePath, resInfo.ModTime, resInfo.Size)
			checkErr(err)

			// fmt.Println("Found cache: " + shaVal + ";" + strconv.FormatInt(lastSeenTime, 10))
			return resInfo, true, nil
		}
	}

	f, err := os.Open(file)
	if err != nil {
		return fileInfo{}, false, err
	}
	defer f.Close()

	hasher := sha512.New()

	if _, err := io.Copy(hasher, f); err != nil {
		return fileInfo{}, false, err
	}

	sha512 := hex.EncodeToString(hasher.Sum(nil))
	resInfo.ChunkKey = "chunk/sha512/" + sha512 + ".deflate"
	return resInfo, false, nil
}

func getOSSClient(conf *userConfig) (client *oss.Client, bucket *oss.Bucket, err error) {
	client, err = oss.New(conf.Oss.APIPrefix, conf.Oss.OssKey, conf.Oss.OssSecret) // oss-cn-hangzhou.aliyuncs.com
	if err != nil {
		return
	}

	bucket, err = client.Bucket(conf.Oss.BucketName) // cloudstorage
	return
}

func updateOnlineChunkList(bucket *oss.Bucket) error {
	fmt.Print("Update Online Chunk List...")
	marker := oss.Marker("")
	onlineChunksSet = make(map[string]bool)

	for {
		lsRes, err := bucket.ListObjects(oss.Prefix("chunk/sha512/"), oss.MaxKeys(1000), marker)
		checkErr(err)
		marker = oss.Marker(lsRes.NextMarker)

		for _, object := range lsRes.Objects {
			onlineChunksSet[object.Key] = true
		}

		if !lsRes.IsTruncated {
			break
		}
	}

	fmt.Printf("%d chunks found\n", len(onlineChunksSet))
	return nil
}

func uploadFileToOSS(p *uploadFileParams) {
	fullPath := filepath.Join(p.basepath, p.fileHashInfo.Path)

	// compress
	compressedFileName, compressedSize := compressFile(fullPath)
	defer os.Remove(compressedFileName)

	// upload
	var compressionRatio float64

	if p.fileHashInfo.Size > 0 {
		compressionRatio = float64(p.fileHashInfo.Size-compressedSize) / float64(p.fileHashInfo.Size) * 100
	}

	err := p.bucket.PutObjectFromFile(p.fileHashInfo.ChunkKey, compressedFileName)
	checkErr(err)

	fmt.Printf("[%d / %d] %s (%s)\n(%.1f%s Compressed) Uploaded\n", p.position, p.totalCount, p.fileHashInfo.Path, formatFileSize(p.fileHashInfo.Size), compressionRatio, "%")
}

func compressFile(filepath string) (tmpPath string, compressedSize int64) {
	// 打开待压缩文件
	f, err := os.Open(filepath)
	checkErr(err)
	defer f.Close()

	// 创建临时文件
	tmpFile, err := ioutil.TempFile("", "ossCompTmp")
	checkErr(err)

	// 创建一个flate.Writer，压缩级别为 3 （偏重速度）
	flateWrite, err := flate.NewWriter(tmpFile, 3) // -2 ~ 9
	checkErr(err)
	defer flateWrite.Close()

	io.Copy(flateWrite, f)
	flateWrite.Flush()

	stat, err := tmpFile.Stat()
	checkErr(err)
	compressedSize = stat.Size()

	return tmpFile.Name(), compressedSize
}

func uploadIndexFile(indexFilePath string, bucket *oss.Bucket) {
	fmt.Printf("Compressing Index...")

	compressedFileName, size := compressFile(indexFilePath)
	defer os.Remove(compressedFileName)

	fmt.Printf("(%s)...Uploading...", formatFileSize(size))

	err := bucket.PutObjectFromFile("indexes/"+strings.Replace(time.Now().Format("2006-01-02T15_04_05.999999999Z07:00"), ":", "_", 1)+".dat.deflate", compressedFileName)
	if err != nil {
		checkErr(err)
	}

	fmt.Println("Done")
}

func formatFileSize(size int64) string {
	if size < 1024 { // < 1 KB
		return strconv.FormatInt(size, 10) + " Bytes"
	}
	if size < 1024*1024 { // < 1 MB
		return strconv.FormatFloat(float64(size)/1024, 'f', 1, 64) + " KB"
	}
	if size < 1024*1024*1024 { // < 1 GB
		return strconv.FormatFloat(float64(size)/1024/1024, 'f', 1, 64) + " MB"
	}

	return strconv.FormatFloat(float64(size)/1024/1024/1024, 'f', 1, 64) + " GB"
}

func processSingleFileScan(conf *userConfig, fullPath string, trx *sql.Tx, writer *bufio.Writer) {
	fileName := filepath.Base(fullPath)

	// ignore index file
	if strings.HasPrefix(fileName, ".__ossIndex_special_.") && strings.HasSuffix(fileName, ".dat") {
		return
	}

	relativePath, _ := filepath.Rel(conf.FileRootPath, fullPath)
	relativePath = filepath.ToSlash(relativePath)

	// get hash
	fileCounter++

	hashInfo, fromCache, err := getFileHashInfo(fullPath, relativePath, true, trx)

	if logLevel == 0 || !fromCache || err != nil || fileCounter%500 == 0 {
		fmt.Printf("[%d] %s\n", fileCounter, relativePath)
	}
	if err != nil {
		// if some file could not be processed, just ignore it :)
		fmt.Printf("[Error] File could not be processed: ")
		fmt.Println(err)

		return
	}

	jsonRow, _ := json.Marshal(hashInfo)
	writer.Write(jsonRow)
	writer.WriteString("\n")

	// add to cache
	if !fromCache {
		_, err = trx.Exec("INSERT INTO index_cache (path, modTime, size, sha512, lastSeenTime) VALUES (?, ?, ?, ?, ?)", relativePath, hashInfo.ModTime, hashInfo.Size, hashInfo.ChunkKey, time.Now().UnixNano())
		checkErr(err)
	}
}

func makeDirIndex(conf *userConfig, bucket *oss.Bucket) (indexFilePath string) {
	path := conf.FileRootPath
	initCache(path)
	basePath, _ := filepath.Abs(path)
	startTime := time.Now()

	fmt.Println("Indexing: " + basePath)

	// 创建临时索引文件
	indexFile, err := ioutil.TempFile("", "ossIndexTmp")
	checkErr(err)
	indexFilePath = indexFile.Name()

	file, err := os.OpenFile(indexFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	checkErr(err)
	defer file.Close()
	writer := bufio.NewWriterSize(file, 4096)

	if err != nil {
		log.Fatal(err)
	}

	trx, _ := cacheDB.Begin()
	defer trx.Commit()
	lastFlushTime := time.Now()

	flushFunc := func() {
		flushStartTime := time.Now()
		writer.Flush()
		trx.Commit()
		trx, err = cacheDB.Begin()
		checkErr(err)

		// fmt.Println("[Flush] " + time.Since(flushStartTime).String())
		lastFlushTime = flushStartTime
	}

	err = godirwalk.Walk(basePath, &godirwalk.Options{
		Callback: func(fullPath string, f *godirwalk.Dirent) error {
			if time.Since(lastFlushTime).Seconds() > 5 {
				flushFunc()
			}

			if !f.IsDir() {
				processSingleFileScan(conf, fullPath, trx, writer)
			}

			return nil
		},
	})

	flushFunc()
	fmt.Println("Finish indexing in " + time.Since(startTime).String())
	return
}

type uploadFileParams struct {
	position     int
	basepath     string
	fileHashInfo *fileInfo
	bucket       *oss.Bucket
	totalCount   int
}

func uploadChangedFiles(basePath string, indexPath string, bucket *oss.Bucket) {
	i := 0

	var wg sync.WaitGroup

	pool, _ := ants.NewPoolWithFunc(12, func(payload interface{}) {
		params, ok := payload.(*uploadFileParams)
		if !ok {
			return
		}
		uploadFileToOSS(params)
		wg.Done()
	})
	defer pool.Release()

	// stats
	countToUpload := 0
	sizeToUpload = int64(0)

	scanFileJSONLines(indexPath, func(line *fileInfo) {
		// check exsitance on OSS
		if !onlineChunksSet[line.ChunkKey] {
			countToUpload++
			sizeToUpload += line.Size
		}
	})

	scanFileJSONLines(indexPath, func(line *fileInfo) {
		// check exsitance on OSS
		if !onlineChunksSet[line.ChunkKey] {
			i++
			wg.Add(1)
			pool.Invoke(&uploadFileParams{
				position:     i,
				basepath:     basePath,
				fileHashInfo: line,
				bucket:       bucket,
				totalCount:   countToUpload,
			})
		}
	})

	wg.Wait()
}

func fullSync(configPath string) {
	conf := getConfig(configPath)
	// "F:\\kindle伴侣同步" // "D:\\NAS-HOME"
	_, bucket, err := getOSSClient(&conf)
	checkErr(err)

	updateOnlineChunkList(bucket)

	indexPath := makeDirIndex(&conf, bucket)
	defer os.Remove(indexPath)

	uploadIndexFile(indexPath, bucket)
	uploadChangedFiles(conf.FileRootPath, indexPath, bucket)
}

func downloadCompressedFile(p *downloadFileParams) (string, int64, error) {
	os.MkdirAll(filepath.Dir(p.localLocation), 755)

	localFile, err := os.OpenFile(p.localLocation, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644) // O_EXCL 代表文件必须不存在，存在则报错
	if err != nil {
		return "", 0, err
	}
	defer localFile.Close()

	// 创建临时文件
	tmpFile, err := ioutil.TempFile("", "ossDownTmp")
	checkErr(err)
	tmpFileName := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpFileName)

	// 下载到该文件
	if err := p.bucket.GetObjectToFile(p.key, tmpFileName); err != nil {
		checkErr(err)
	}

	// 解压文件
	tmpFile, err = os.Open(tmpFileName)
	checkErr(err)
	defer tmpFile.Close()

	flateRead := flate.NewReader(tmpFile)
	writer := bufio.NewWriter(localFile)

	defer flateRead.Close()
	io.Copy(writer, flateRead)
	writer.Flush()

	size, _ := localFile.Seek(0, 1)

	return p.localLocation, size, nil
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: ossBackup [-r] [-s] [-h] [-t timestamp] [-p restorePath]

Options:
`)
	flag.PrintDefaults()
}

func restoreFiles(configFileName string, path string, time string) {
	conf := getConfig(configFileName)
	_, bucket, err := getOSSClient(&conf)
	checkErr(err)

	fmt.Print("Downloading index...")

	indexFile, err := ioutil.TempFile("", "ossIndexTmp")
	checkErr(err)
	indexPath := indexFile.Name()
	indexFile.Close()
	os.Remove(indexPath)
	defer os.Remove(indexPath)

	_, indexSize, err := downloadCompressedFile(&downloadFileParams{
		bucket:        bucket,
		key:           "indexes/" + time + ".dat.deflate",
		localLocation: indexPath,
	})
	checkErr(err)

	fmt.Printf("Done (%s)\n", formatFileSize(indexSize))

	downloadAllOSSFilesInIndex(&conf, path, bucket, indexPath)
}

type downloadFileParams struct {
	bucket        *oss.Bucket
	key           string
	localLocation string
}

type downloadFileTask struct {
	downloadParams *downloadFileParams
	info           *fileInfo
}

func downloadAllOSSFilesInIndex(conf *userConfig, restoreToPath string, bucket *oss.Bucket, indexPath string) {
	// 第一遍扫描，确定需要下载的文件数量和总大小
	var totalCount int32
	var totalSize int64
	var downloadedCount int64

	scanFileJSONLines(indexPath, func(line *fileInfo) {
		totalCount++
		totalSize += line.Size
	})

	fmt.Printf("Starting downloading %v files (%v)\n", totalCount, formatFileSize(totalSize))

	var wg sync.WaitGroup

	pool, _ := ants.NewPoolWithFunc(12, func(payload interface{}) {
		params, ok := payload.(*downloadFileTask)
		if !ok {
			return
		}

		_, size, err := downloadCompressedFile(params.downloadParams)

		atomic.AddInt64(&downloadedCount, params.info.Size)
		relativePath, _ := filepath.Rel(restoreToPath, params.downloadParams.localLocation)

		if err == nil {
			os.Chtimes(params.downloadParams.localLocation, time.Unix(0, params.info.ModTime), time.Unix(0, params.info.ModTime))
			fmt.Printf("(%s / %s) Downloaded %s (%s)\n", formatFileSize(downloadedCount), formatFileSize(totalSize), relativePath, formatFileSize(size))
		} else {
			fmt.Printf("(%s / %s) Ignored %s: %v\n", formatFileSize(downloadedCount), formatFileSize(totalSize), relativePath, err)
		}

		wg.Done()
	})
	defer pool.Release()

	// 第二遍扫描，开始下载
	scanFileJSONLines(indexPath, func(line *fileInfo) {
		fullPath := filepath.Join(restoreToPath, filepath.FromSlash(line.Path))

		wg.Add(1)
		pool.Invoke(&downloadFileTask{
			downloadParams: &downloadFileParams{
				bucket, line.ChunkKey, fullPath,
			},
			info: line,
		})
	})

	wg.Wait()
}

func scanFileJSONLines(path string, processer func(line *fileInfo)) {
	f, err := os.Open(path)
	checkErr(err)
	defer f.Close()

	reader := bufio.NewReaderSize(f, 10240)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer([]byte{}, bufio.MaxScanTokenSize*10)

	for scanner.Scan() {
		bytes := scanner.Bytes()

		var line fileInfo

		if err := json.Unmarshal(bytes, &line); err != nil {
			fmt.Println(scanner.Text())
			panic(err)
		}

		processer(&line)
	}

	if err := scanner.Err(); err != nil {
		checkErr(err)
	}
}

func parseCmd() {
	var restore bool
	var sync bool
	var help bool
	var time string
	var path string
	var configFileName string
	flag.BoolVar(&restore, "r", false, "restore files from OSS")
	flag.BoolVar(&sync, "s", false, "sync files to OSS")
	flag.BoolVar(&help, "h", false, "show help and exit")
	flag.StringVar(&time, "t", "", "the timestamp for restoring files (like 2019-08-02T02_44_44.7450746+08_00)")
	flag.StringVar(&path, "p", "", "the path for restoring files (required for restoring)")
	flag.StringVar(&configFileName, "c", "", "the name of config file")

	// 改变默认的 Usage
	flag.Usage = usage

	flag.Parse() // Scans the arg list and sets up flags

	if sync {
		fullSync(configFileName)
	} else if restore && path != "" && time != "" {
		restoreFiles(configFileName, path, time)
	} else {
		flag.Usage()
	}
}

func main() {
	fmt.Println("OssArchiveStorageBackup " + version)
	parseCmd()
}
