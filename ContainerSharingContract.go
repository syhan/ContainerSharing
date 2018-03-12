package main

import (
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/protos/peer"
	"encoding/json"
)

type ContainerSharingContract struct {

}

type Request struct {
	Id			string	`json:"id"`
	Type		string	`json:"type"`
	Vendor		string 	`json:"vendor"`
	FromDate	int64	`json:"from"`
	ToDate		int64	`json:"to"`
	TEU			uint32	`json:"teu"`
	Constraints	string	`json:"constrains"`		
}

func SaveRequest(stub shim.ChaincodeStubInterface, request Request) error {
	if request.Id == "" {
		request.Id = stub.GetTxID()
	}

	key, e := stub.CreateCompositeKey(request.Type, []string{request.Id})
	if e != nil {
		return fmt.Errorf("Error creating composite key")
	}

	bytes, e := json.Marshal(request)
	if e != nil {
		return fmt.Errorf("Error marshalling request to json")
	}

	err := stub.PutState(key, bytes)
	if (err != nil) {
		return fmt.Errorf("Fail to save request")
	}

	return nil
}

func Match(stub shim.ChaincodeStubInterface, request Request) error {
	var key = "Demand"
	if (request.Type == key) {
		key = "Supply"
	}

	iter, err := stub.GetStateByPartialCompositeKey(key, []string{})
	defer iter.Close()

	if err != nil {
		return fmt.Errorf("Error getting state by partial composite key")
	}

	for iter.HasNext() {
		r, e := iter.Next()

		if e != nil {
			return fmt.Errorf("Error getting next")
		}

		/*_, _, e := stub.SplitCompositeKey(r.Key)
		if e != nil {
			return fmt.Errorf("Error split composite key")
		}

		value, e := stub.GetState(k)
		if e != nil {
			return fmt.Errorf("Error get state")
		}*/

		var req Request
		e = json.Unmarshal(r.Value, &req)
		if e != nil {
			return fmt.Errorf(fmt.Sprintf("Error unmarshal %s", e.Error()))
		}

		if (req.TEU >= request.TEU && req.FromDate <= request.FromDate && req.ToDate >= request.ToDate) {
			req.TEU = req.TEU - request.TEU

			error := SaveRequest(stub, req)
			if error != nil {
				return error
			}

			break
		}
	}

	return nil
}

func PostRequest(stub shim.ChaincodeStubInterface, args []string) peer.Response {
	if len(args) != 1 {
		return shim.Error("Incorrect argument, expect a json payload")
	}

	var request Request
	err := json.Unmarshal([]byte(args[0]), &request)

	if err != nil {
		return shim.Error(fmt.Sprintf("Malformed json payload: %s", args[0]))
	}

	err = SaveRequest(stub, request)
	if err != nil {
		return shim.Error(fmt.Sprintf("Save request failed due to %s", err.Error()))
	}

	err = Match(stub, request)
	if err != nil {
		return shim.Error(fmt.Sprintf("Match request failed due to %s", err.Error()))
	}

	return shim.Success([]byte("Successfully post request to hyperledger"))
}

func (t *ContainerSharingContract) Init(stub shim.ChaincodeStubInterface) peer.Response {
    return shim.Success([]byte("Successfully deploy container sharing chaincode"))
}

func (t *ContainerSharingContract) Invoke(stub shim.ChaincodeStubInterface) peer.Response {
	fn, args := stub.GetFunctionAndParameters()
	
	if fn == "PostRequest" {
		return PostRequest(stub, args)
	}

	return shim.Error(fmt.Sprintf("Unknown method %s with args %s invocation", fn, args))
}


func main() {
	if err := shim.Start(new(ContainerSharingContract)); err != nil {
		fmt.Printf("Error create new Container Sharing Contract")
	}
}