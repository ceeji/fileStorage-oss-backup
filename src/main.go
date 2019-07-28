/**
 * Using Aliyun's Archive Storage (OSS) to backup all files in a directory
 * This is a good way to be used along with Private Cloud Storage
 * To get a cheap and reliable storage
 */
package main

import (
	"bufio"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
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

// import "path/filepath"

/*
 * generate hash information of a file
 * if fastMode is true, sha512 cache will be used according to file last-modified-time and file path.
 */
func getFileHashInfo(file string, relativePath string, fastMode bool) (fileInfo, error) {
	f, err := os.Open(file)
	if err != nil {
		return fileInfo{}, err
	}
	defer f.Close()

	hasher := sha512.New()

	if _, err := io.Copy(hasher, f); err != nil {
		return fileInfo{}, err
	}

	sha512 := hex.EncodeToString(hasher.Sum(nil))

	stat, err := f.Stat()
	fileTime := times.Get(stat)

	return fileInfo{
		Path:         relativePath,
		ChunkKey:     "chunk/sha512/" + sha512,
		Size:         stat.Size(),
		ModTime:      stat.ModTime().UnixNano(),
		CreationTime: fileTime.BirthTime().UnixNano(),
	}, nil
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
		if err != nil {
			return err
		}
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

func checkAndUploadFileToOSS(basepath string, fileHashInfo fileInfo, bucket *oss.Bucket) {
	// fmt.Println("check exsitance")
	exsits := onlineChunksSet[fileHashInfo.ChunkKey]

	if exsits {
		return // nothing to do
	}

	// upload
	fmt.Printf("Uploading...(%s)", formatFileSize(fileHashInfo.Size))
	err := bucket.PutObjectFromFile(fileHashInfo.ChunkKey, filepath.Join(basepath, fileHashInfo.Path))
	if err != nil {
		fmt.Print(err)
	}
	fmt.Println()
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

func walkDir(path string, bucket *oss.Bucket) {
	basePath, _ := filepath.Abs(path)

	fmt.Println("Start walking:" + basePath)

	filepath.Walk(basePath,
		func(path string, f os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			fullPath := path
			relativePath, _ := filepath.Rel(basePath, fullPath)
			relativePath = filepath.ToSlash(relativePath)

			if !f.IsDir() {
				// get hash
				fileCounter++
				fmt.Printf("(%d) %s ", fileCounter, relativePath)

				hashInfo, err := getFileHashInfo(basePath, fullPath, false)

				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return nil
				}
				fmt.Printf("%s\n", hashInfo.ChunkKey)

				checkAndUploadFileToOSS(basePath, hashInfo, bucket)
			}

			return nil
		})
}

func makeDirIndex(path string, bucket *oss.Bucket) {
	basePath, _ := filepath.Abs(path)

	fmt.Println("Indexing:" + basePath)

	file, err := os.OpenFile(filepath.Join(basePath, ".ossIndex."+strconv.FormatInt(time.Now().Unix(), 10)+".dat"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	writer := bufio.NewWriterSize(file, 4096)

	if err != nil {
		log.Fatal(err)
	}

	filepath.Walk(basePath,
		func(path string, f os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			fullPath := path
			relativePath, _ := filepath.Rel(basePath, fullPath)
			relativePath = filepath.ToSlash(relativePath)

			if !f.IsDir() {
				// get hash
				fileCounter++
				fmt.Printf("(%d) %s ", fileCounter, relativePath)

				hashInfo, err := getFileHashInfo(fullPath, relativePath, false)

				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return nil
				}
				fmt.Printf("%s\n", hashInfo.ChunkKey)

				jsonRow, _ := json.Marshal(hashInfo)
				writer.Write(jsonRow)
				writer.WriteString("\n")
			}

			return nil
		})

	writer.Flush()
	file.Close()
}

func main() {
	fmt.Println("OssArchiveStorageBackup v0.1")

	_, bucket, err := getOSSClient()

	if err != nil {
		fmt.Println(err)
		return
	}

	updateOnlineChunkList(bucket)
	makeDirIndex("Z:\\", bucket)
	// walkDir("Z:\\", bucket)
	// testOSS()
}
