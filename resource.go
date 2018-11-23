package main

import (
	"flag"
	"fmt"
	"strconv"
	"sync"
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
	NumCalls   int    `json:"num_calls"`
}

func main() {
	// New dynamo session and client...
	sess := session.Must(session.NewSession())

	svc := dynamodb.New(sess)

	load := flag.Bool("l", false, "load dynamo with generated records")
	safe := flag.Bool("s", false, "use a safe conditional write to dynamo")

	flag.Parse()

	if *load {
		fmt.Println("loading tables with data...")
		err := loadTable(svc)
		if err != nil {
			fmt.Println("Error loading the table:", err)
		}
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)

		go func(resourceID string, routineID int) {
			t := time.Now()
			numReadErrs := 0
			numWriteErrs := 0
			numWrites := 0
			// Decrement the counter when the goroutine completes.
			defer wg.Done()
			// Do work
			// Read an agent
			for numWrites < 10 {
				item, err := readAgent(svc, resourceID)
				if err != nil {
					fmt.Println("Error reading the agent:", err)
					numReadErrs = numReadErrs + 1
					continue
				}

				err = incrementCalls(svc, item, *safe)
				if err != nil {
					numWriteErrs = numWriteErrs + 1
					continue
				} else {
					numWrites = numWrites + 1
				}
			}

			elapsed := time.Since(t)

			msg := fmt.Sprintln("routine ", routineID, " writes:", numWrites, " read errs: ", numReadErrs, " write errs: ", numWriteErrs, " elapsed: ", elapsed)
			fmt.Printf(msg)
		}("100", i)

	}
	// Wait for all groups to complete
	wg.Wait()

	// Read an agent
	agent, err := readAgent(svc, "100")
	if err != nil {
		fmt.Println("Error reading the table:", err)
		return
	}

	fmt.Println("Final state:")
	fmt.Println("ResourceID:  ", agent.ResourceID)
	fmt.Println("AccountID:  ", agent.AccountID)
	fmt.Println("Status: ", agent.Status)
	fmt.Println("Num calls:  ", agent.NumCalls)

}

type numCallsCondition struct {
	NumCalls int `json:":num_calls"`
}

func incrementCalls(svc *dynamodb.DynamoDB, item *Resource, safeUpdate bool) error {
	var input *dynamodb.PutItemInput

	if safeUpdate {
		conditionalExp := aws.String("num_calls = :num_calls")

		condition, err := dynamodbattribute.MarshalMap(numCallsCondition{
			NumCalls: item.NumCalls,
		})

		if err != nil {
			fmt.Println(err)
			return err
		}

		input = &dynamodb.PutItemInput{
			Item: map[string]*dynamodb.AttributeValue{
				"resource_id": {
					S: aws.String(item.ResourceID),
				},
				"account_id": {
					S: aws.String(item.AccountID),
				},
				"available": {
					BOOL: aws.Bool(item.Available),
				},
				"status": {
					S: aws.String(item.Status),
				},
				"num_calls": {
					// Add one and convert to string for writing
					N: aws.String(strconv.Itoa(item.NumCalls + 1)),
				}},
			ReturnConsumedCapacity:    aws.String("TOTAL"),
			TableName:                 aws.String(Table),
			ConditionExpression:       conditionalExp,
			ExpressionAttributeValues: condition,
		}
	} else {

		input = &dynamodb.PutItemInput{
			Item: map[string]*dynamodb.AttributeValue{
				"resource_id": {
					S: aws.String(item.ResourceID),
				},
				"account_id": {
					S: aws.String(item.AccountID),
				},
				"available": {
					BOOL: aws.Bool(item.Available),
				},
				"status": {
					S: aws.String(item.Status),
				},
				"num_calls": {
					// Add one and convert to string for writing
					N: aws.String(strconv.Itoa(item.NumCalls + 1)),
				}},
			ReturnConsumedCapacity: aws.String("TOTAL"),
			TableName:              aws.String(Table),
		}
	}

	_, err := svc.PutItem(input)
	if err != nil {
		return err
	}

	return nil
}

func readAgent(svc *dynamodb.DynamoDB, resourceID string) (*Resource, error) {
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
		return nil, err
	}

	r := Resource{}

	err = dynamodbattribute.UnmarshalMap(result.Item, &r)

	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

	return &r, nil
}

func loadTable(svc *dynamodb.DynamoDB) error {
	var resources []Resource

	for i := 0; i < 5; i++ {

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
