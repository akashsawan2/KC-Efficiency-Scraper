package controller

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"kubecost-efficiency-fetcher/configs"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"regexp"
)

func FetchAndWriteControllerData(inputURL, clusterName, window, bucketName, region string, wg *sync.WaitGroup) {

	defer wg.Done()
	u, err := url.Parse(inputURL)
	if err != nil {
		configs.ErrorLogger.Println("Error parsing URL:", err)
		return
	}
	u.Path = "/model/allocation"
	q := u.Query()
	q.Set("window", window)
	q.Set("aggregate", "controller")
	q.Set("accumulate", "true")
	u.RawQuery = q.Encode()
	newURL := u.String()

	var resp *http.Response
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err = http.Get(newURL)
		if err == nil {
			break
		}
		configs.ErrorLogger.Printf("Attempt %d: Error making HTTP request: %v\n", attempt, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		configs.ErrorLogger.Println("Failed to make HTTP request after multiple attempts:", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		configs.ErrorLogger.Println("Error reading response body:", err)
		return
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		configs.ErrorLogger.Println("Error unmarshalling JSON:", err)
		return
	}
	configs.InfoLogger.Println("Status Code for Controller: ", result["code"])

	data := result["data"].([]interface{})

	svc := configs.Svc

	objectKeyController := "Controller/Controller.csv"
	objectKeyRollout := "Rollout/Rollout.csv"

	existingData := [][]string{}
	rolloutData := [][]string{}
	fileExistsController := false
	fileExistsRollout := false

	_, err = svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKeyController),
	})
	if err == nil {
		resp, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKeyController),
		})
		if err != nil {
			configs.ErrorLogger.Println("Error fetching existing Controller file from S3:", err)
			return
		}
		defer resp.Body.Close()

		reader := csv.NewReader(resp.Body)
		existingData, err = reader.ReadAll()
		if err != nil {
			configs.ErrorLogger.Println("Error reading existing Controller CSV data:", err)
			return
		}
		fileExistsController = true
	} else {
		configs.InfoLogger.Println("No existing Controller.csv file found. A new one will be created.")
	}

	_, err = svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKeyRollout),
	})
	if err == nil {
		resp, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKeyRollout),
		})
		if err != nil {
			configs.ErrorLogger.Println("Error fetching existing Rollout file from S3:", err)
			return
		}
		defer resp.Body.Close()

		reader := csv.NewReader(resp.Body)
		rolloutData, err = reader.ReadAll()
		if err != nil {
			configs.ErrorLogger.Println("Error reading existing Rollout CSV data:", err)
			return
		}
		fileExistsRollout = true
	} else {
		configs.InfoLogger.Println("No existing Rollout.csv file found. A new one will be created.")
	}

	var bufferController, bufferRollout bytes.Buffer
	writerController := csv.NewWriter(&bufferController)
	writerRollout := csv.NewWriter(&bufferRollout)
	defer writerController.Flush()
	defer writerRollout.Flush()

	if !fileExistsController {
		header := []string{"Controller", "ClusterName", "Region", "Namespace", "Window Start", "Window End", "Cpu Cost", "Gpu Cost", "Ram Cost", "PV Cost", "Network Cost", "LoadBalancer Cost", "Total Cost", "Cpu Efficiency", "Ram Efficiency", "Total Efficiency"}
		if err := writerController.Write(header); err != nil {
			configs.ErrorLogger.Println("Error writing header to Controller CSV:", err)
			return
		}
	}
	if !fileExistsRollout {
		header := []string{"Rollout", "ClusterName", "Region", "Namespace", "Window Start", "Window End", "Cpu Cost", "Gpu Cost", "Ram Cost", "PV Cost", "Network Cost", "LoadBalancer Cost", "Total Cost", "Cpu Efficiency", "Ram Efficiency", "Total Efficiency"}
		if err := writerRollout.Write(header); err != nil {
			configs.ErrorLogger.Println("Error writing header to Rollout CSV:", err)
			return
		}
	}

	re := regexp.MustCompile(`-[^-]+$`)

	for _, element := range data {
		if element == nil {
			configs.InfoLogger.Println("No Data for Controller")
			continue
		}
		controllerMap := element.(map[string]interface{})

		for _, controllerData := range controllerMap {
			controllerOne := controllerData.(map[string]interface{})

			name := controllerOne["name"].(string)
			if name == "__unallocated__" {
				continue
			}
			name = re.ReplaceAllString(name, "")

			properties := controllerOne["properties"].(map[string]interface{})

			var labels map[string]interface{}
			var region string
			var namespaceController string

			if value, ok := properties["namespace"].(string); ok {
				namespaceController = value
			} else {
				namespaceController = ""
			}

			if value, ok := properties["labels"].(map[string]interface{}); ok {
				labels = value
				if val_region, ok := labels["topology_kubernetes_io_region"].(string); ok {
					region = val_region
				} else {
					region = ""
				}
			} else {
				region = ""
			}

			window := controllerOne["window"].(map[string]interface{})
			windowStart := window["start"].(string)
			windowEnd := window["end"].(string)

			cpuCost := controllerOne["cpuCost"].(float64)
			gpuCost := controllerOne["gpuCost"].(float64)
			ramCost := controllerOne["ramCost"].(float64)
			pvCost := controllerOne["pvCost"].(float64)
			networkCost := controllerOne["networkCost"].(float64)
			loadBalancerCost := controllerOne["loadBalancerCost"].(float64)
			totalCost := controllerOne["totalCost"].(float64)
			cpuEfficiency := controllerOne["cpuEfficiency"].(float64) * 100
			ramEfficiency := controllerOne["ramEfficiency"].(float64) * 100
			totalEfficiency := controllerOne["totalEfficiency"].(float64) * 100

			record := []string{
				name, clusterName, region, namespaceController, windowStart, windowEnd,
				fmt.Sprintf("%f", cpuCost), fmt.Sprintf("%f", gpuCost),
				fmt.Sprintf("%f", ramCost), fmt.Sprintf("%f", pvCost),
				fmt.Sprintf("%f", networkCost), fmt.Sprintf("%f", loadBalancerCost),
				fmt.Sprintf("%f", totalCost),
				fmt.Sprintf("%f", cpuEfficiency), fmt.Sprintf("%f", ramEfficiency),
				fmt.Sprintf("%f", totalEfficiency),
			}

			existingData = append(existingData, record)

			if strings.HasPrefix(name, "rollout:") {
				nameWithoutRollout := strings.TrimPrefix(name, "rollout:")
				
				rolloutRecord := make([]string, len(record))
				copy(rolloutRecord, record)                  
				rolloutRecord[0] = nameWithoutRollout     
				rolloutData = append(rolloutData, rolloutRecord)
			}

		}

		if err := writerController.WriteAll(existingData); err != nil {
			configs.ErrorLogger.Println("Error writing Controller data to CSV:", err)
			return
		}

		if err := writerRollout.WriteAll(rolloutData); err != nil {
			configs.ErrorLogger.Println("Error writing Rollout data to CSV:", err)
			return
		}

		err = os.MkdirAll("Output", 0755)
		if err != nil {
			configs.ErrorLogger.Println("Error creating directory:", err)
			return
		}

		err = os.WriteFile("Output/Controller.csv", bufferController.Bytes(), 0644)
		if err != nil {
			configs.ErrorLogger.Println("Error saving Controller.csv file locally:", err)
			return
		}

		err = os.WriteFile("Output/Rollout.csv", bufferRollout.Bytes(), 0644)
		if err != nil {
			configs.ErrorLogger.Println("Error saving Rollout.csv file locally:", err)
			return
		}

		_, err = svc.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKeyController),
			Body:   bytes.NewReader(bufferController.Bytes()),
		})
		if err != nil {
			configs.ErrorLogger.Println("Error uploading Controller.csv file to S3:", err)
			return
		}

		_, err = svc.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKeyRollout),
			Body:   bytes.NewReader(bufferRollout.Bytes()),
		})
		if err != nil {
			configs.ErrorLogger.Println("Error uploading Rollout.csv file to S3:", err)
			return
		}

		configs.InfoLogger.Println("Controller and Rollout data successfully written to S3.")
	}
}