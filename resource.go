package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Table is the dynamo table we're working against.
const Table = "resources"

// AccountID ...
const AccountID = "10001"

// Resource represents something like an agent etc.
type Resource struct {
	ResourceID string `json:"resource_id"`
	AccountID  string `json:"account_id"`
	Available  bool   `json:"available"`
	Status     string `json:"status"`
	NumCalls   int64  `json:"num_calls"`
}

func main() {
	// New dynamo session and client...
	sess := session.Must(session.NewSession())

	svc := dynamodb.New(sess)

	load := flag.Bool("l", false, "load dynamo with generated records")

	if *load {
		fmt.Println("loading tables with data...")
		err := loadTable(svc)
		if err != nil {
			fmt.Println("Error loading the table:", err)
		}
	}

	// Read an item by key
	err := readItem(svc, "100")
	if err != nil {
		fmt.Println("Error reading the table:", err)
	}

}

func readItem(svc *dynamodb.DynamoDB, resourceID string) error {
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(Table),
		Key: map[string]*dynamodb.AttributeValue{
			"resource_id": {
				S: aws.String(resourceID),
			},
			"account_id": {
				S: aws.String(AccountID),
			},
		},
	})

	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	r := Resource{}

	err = dynamodbattribute.UnmarshalMap(result.Item, &r)

	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

	// if item.Title == "" {
	// 	fmt.Println("Could not find the resource with ID ", resourceID)
	// 	return err
	// }

	fmt.Println("Found item:")
	fmt.Println("ResourceID:  ", r.ResourceID)
	fmt.Println("AccountID:  ", r.AccountID)
	fmt.Println("Status: ", r.Status)
	fmt.Println("Num calls:  ", r.NumCalls)

	return nil
}

func loadTable(svc *dynamodb.DynamoDB) error {
	var resources []Resource

	for i := 0; i < 5; i++ {
		time.Sleep(time.Millisecond * 5) // make sure we have some slew in timestamps

		resource := Resource{
			ResourceID: fmt.Sprintf("%d", 100+i),
			AccountID:  AccountID,
			Available:  false,
			Status:     "offline",
			NumCalls:   0,
		}
		resources = append(resources, resource)
	}

	for _, r := range resources {
		values, err := dynamodbattribute.MarshalMap(r)

		if err != nil {
			fmt.Printf("Got error calling MarshalMap: %v\n", err)
			return err
		}

		inputPut := &dynamodb.PutItemInput{
			Item:      values,
			TableName: aws.String("resources"),
		}

		_, err = svc.PutItem(inputPut)

		if err != nil {
			fmt.Printf("Got error calling PutItem: %v\n", err)
			return err
		}

		fmt.Println("Successfully added ", r.ResourceID, " to table")
	}
	return nil
}
