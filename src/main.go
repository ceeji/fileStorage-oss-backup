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
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/karrick/godirwalk"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/djherbis/times.v1"
)

type fileInfo struct {
	Path         string
	ChunkKey     string
	Size         int64
	ModTime      int64
	CreationTime int64
}

var fileCounter int
var onlineChunksSet map[string]bool
var logLevel int8 = 1 // 0: verbose 1:info 2: none
var cacheDB *sql.DB

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func initCache(basepath string) {
	// 打开数据库，如果不存在，则创建
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(basepath, ".ossIndex.cache.db"))
	cacheDB = db
	checkErr(err)

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
		var lastSeenTime int64
		row := tx.QueryRow("SELECT sha512, lastSeenTime FROM index_cache WHERE path = ? AND modTime = ? AND size = ?", relativePath, resInfo.ModTime, resInfo.Size)

		if row != nil && row.Scan(&shaVal, &lastSeenTime) == nil {
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

func getOSSClient() (client *oss.Client, bucket *oss.Bucket, err error) {
	client, err = oss.New("oss-cn-beijing.aliyuncs.com", "", "") // oss-cn-hangzhou.aliyuncs.com
	if err != nil {
		return
	}

	bucket, err = client.Bucket("ceeji-test") // cloudstorage
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

func checkAndUploadFileToOSS(position int, basepath string, fileHashInfo fileInfo, bucket *oss.Bucket) {
	exsits := onlineChunksSet[fileHashInfo.ChunkKey]

	if exsits {
		return // nothing to do
	}

	fullPath := filepath.Join(basepath, fileHashInfo.Path)
	fmt.Printf("[%d / %d] %s (%s)\nCompressing...", position, fileCounter, fileHashInfo.Path, formatFileSize(fileHashInfo.Size))

	// compress
	compressedFileName, compressedSize := compressFile(fullPath)
	defer os.Remove(compressedFileName)

	// upload
	var compressionRatio float64

	if fileHashInfo.Size > 0 {
		compressionRatio = float64(fileHashInfo.Size-compressedSize) / float64(fileHashInfo.Size) * 100
	}

	fmt.Printf("(%.1f%s Compressed) Uploading...", compressionRatio, "%")
	err := bucket.PutObjectFromFile(fileHashInfo.ChunkKey, compressedFileName)
	checkErr(err)

	fmt.Println()
}

func compressFile(filepath string) (tmpPath string, compressedSize int64) {
	// 打开待压缩文件
	f, err := os.Open(filepath)
	checkErr(err)
	defer f.Close()

	// 创建临时文件
	tmpFile, err := ioutil.TempFile("", "ossCompTmp")
	checkErr(err)

	// 创建一个flate.Writer，压缩级别为 2 （偏重速度）
	flateWrite, err := flate.NewWriter(tmpFile, 2) // -2 ~ 9
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
	fmt.Printf("Uploading Index " + indexFilePath + "...")

	compressedFileName, _ := compressFile(indexFilePath)
	defer os.Remove(compressedFileName)

	err := bucket.PutObjectFromFile("indexes/"+filepath.Base(indexFilePath)+".deflate", compressedFileName)
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

func makeDirIndex(path string, bucket *oss.Bucket) (indexFilePath string) {
	initCache(path)
	basePath, _ := filepath.Abs(path)
	startTime := time.Now()

	fmt.Println("Indexing:" + basePath)

	indexFilePath = filepath.Join(basePath, ".ossIndex."+strings.Replace(time.Now().Format("2006-01-02T15_04_05.999999999Z07:00"), ":", "_", 1)+".dat")
	fmt.Println("Writing to " + indexFilePath)
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

	err = godirwalk.Walk(basePath, &godirwalk.Options{
		Callback: func(fullPath string, f *godirwalk.Dirent) error {
			if time.Since(lastFlushTime).Seconds() > 5 {
				flushStartTime := time.Now()
				writer.Flush()
				trx.Commit()
				trx, err = cacheDB.Begin()
				checkErr(err)

				fmt.Println("[Flush] " + time.Since(flushStartTime).String())
				lastFlushTime = flushStartTime
			}

			relativePath, _ := filepath.Rel(basePath, fullPath)
			relativePath = filepath.ToSlash(relativePath)

			if !f.IsDir() {
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

					return nil
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

			return nil
		},
	})

	fmt.Println("Finish indexing in " + time.Since(startTime).String())
	return
}

func uploadChangedFiles(basePath string, indexPath string, bucket *oss.Bucket) {
	f, err := os.Open(indexPath)
	checkErr(err)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	i := 0

	for scanner.Scan() {
		// find each file
		var line fileInfo
		json.Unmarshal(scanner.Bytes(), &line)

		// check exsitance on OSS and upload if needed
		i++
		checkAndUploadFileToOSS(i, basePath, line, bucket)
	}
}

func main() {
	scanRoot := "D:\\NAS-HOME" // F:\\kindle伴侣同步

	fmt.Println("OssArchiveStorageBackup v0.1")

	_, bucket, err := getOSSClient()

	if err != nil {
		fmt.Println(err)
		return
	}

	updateOnlineChunkList(bucket)
	indexPath := makeDirIndex(scanRoot, bucket)
	uploadIndexFile(indexPath, bucket)
	uploadChangedFiles(scanRoot, indexPath, bucket)
}
