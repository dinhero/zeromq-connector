package main

import (
	api_connect "github.com/dinhero/zeromq-connector/api/ZeroMQ_Connector"
)

func main() {
	// fmt.Println("This is a line")
	var Zmq api_connect.DWX_ZeroMQ_Connector
	Zmq.Initialize_Connector_Instance("", "", "", 0, 0, 0, "", nil, nil, true, 1000, 0.001, false)
	//ZeroMQ_Connector.Initialize_Connector_Instance("", "", "", 0, 0, 0, "", nil, nil, true, 1000, 0.001, false)

}
