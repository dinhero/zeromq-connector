package DWX_ZMQ_Execution

import (
	"encoding/json"
	"time"
	api_connect "zeromq-connector/api/ZeroMQ_Connector"
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

func (exe *ZMQ_Execution) Init() {
	exe.Zmq.Initialize_Connector_Instance("dwx-zeromq", "localhost", "tcp", 32768, 32769, 32770, ";", map[string]interface{}{}, map[string]interface{}{}, true, 1000, 0.001, false)

}

func (exe *ZMQ_Execution) Execute(_exec_dict map[string]interface{}, _verbose bool, _delay float64, _wbreak float64) map[string]interface{} {
	_check := ""
	//# Reset thread data output
	exe.Zmq.Set_response_(map[string]interface{}{})

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
	for !exe.Zmq.Valid_response_("zmq") {
		// 	sleep(_delay)
		time.Sleep(time.Duration(_delay))

		if float64((time.Now().Unix() - _ws)) > (_delay * _wbreak) {
			break
		}

	}
	// # If data received, return DataFrame
	if exe.Zmq.Valid_response_("zmq") {
		_, exists := exe.Zmq.Get_response_()[_check]
		if exists {
			return exe.Zmq.Get_response_()
		}
	}
	// # Default
	return nil
}
