package DWX_ZMQ_Execution

import (
	"encoding/json"
	"time"

	api_connect "github.com/dinhero/zeromq-connector/api/ZeroMQ_Connector"
)

// # -*- coding: utf-8 -*-
// """
//     DWX_ZMQ_Execution.py
//     --
//     @author: Darwinex Labs (www.darwinex.com)

//     Copyright (c) 2019 onwards, Darwinex. All rights reserved.

//     Licensed under the BSD 3-Clause License, you may not use this file except
//     in compliance with the License.

//     You may obtain a copy of the License at:
//     https://opensource.org/licenses/BSD-3-Clause
// """

type ZMQ_Execution struct {
	Zmq api_connect.DWX_ZeroMQ_Connector
}

func (exe *ZMQ_Execution) Init(ClientID string,
	host string,
	protocol string,
	PUSH_PORT int,
	PULL_PORT int,
	SUB_PORT int,
	delimiter string,
	pulldata_handlers map[string]interface{},
	subdata_handlers map[string]interface{},
	verbose bool,
	poll_timeout int,
	sleep_delay float32,
	monitor bool) {

	exe.Zmq.Initialize_Connector_Instance(ClientID,
		host,
		protocol,
		PUSH_PORT,
		PULL_PORT,
		SUB_PORT,
		delimiter,
		pulldata_handlers,
		subdata_handlers,
		verbose,
		poll_timeout,
		sleep_delay,
		monitor)

}

func (exe *ZMQ_Execution) Execute(_exec_dict map[string]interface{}, _verbose bool, _delay float64, _wbreak float64) map[string]interface{} {
	_check := ""
	//# Reset thread data output
	exe.Zmq.Set_response_(nil)

	//# OPEN TRADE
	if _exec_dict["_action"] == "OPEN" {
		_check = "_action"
		exe.Zmq.DWX_MTX_NEW_TRADE_(_exec_dict)
	} else if _exec_dict["_action"] == "CLOSE" { //# CLOSE TRADE

		_check = "_response_value"
		exe.Zmq.DWX_MTX_CLOSE_TRADE_BY_TICKET_(_exec_dict["_ticket"].(int))
	}

	if _verbose {
		data, _ := json.Marshal(_exec_dict)
		print("[" + _exec_dict["_comment"].(string) + "] " + string(data) + " -> MetaTrader")
	}
	// # While loop start time reference
	_ws := time.Now().Unix()

	// # While data not received, sleep until timeout
	for !exe.Zmq.Valid_response_(map[string]interface{}{"zmq": "zmq"}) {
		// 	sleep(_delay)
		time.Sleep(time.Duration(_delay))

		if float64((time.Now().Unix() - _ws)) > (_delay * _wbreak) {
			break
		}

	}
	// # If data received, return DataFrame
	if exe.Zmq.Valid_response_(map[string]interface{}{"zmq": "zmq"}) {
		_, exists := exe.Zmq.Get_response_()[_check]
		if exists {
			return exe.Zmq.Get_response_()
		}
	}
	// # Default
	return nil
}
