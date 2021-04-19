package DWX_ZMQ_Reporting

// # -*- coding: utf-8 -*-
// """
//     DWX_ZMQ_Reporting.py
//     --
//     @author: Darwinex Labs (www.darwinex.com)

//     Copyright (c) 2019 onwards, Darwinex. All rights reserved.

//     Licensed under the BSD 3-Clause License, you may not use this file except
//     in compliance with the License.

//     You may obtain a copy of the License at:
//     https://opensource.org/licenses/BSD-3-Clause
// """
import (
	api_connect "zeromq-connector/api/ZeroMQ_Connector"
)

type ZMQ_Reporting struct {
	Zmq api_connect.DWX_ZeroMQ_Connector
}

func (r *ZMQ_Reporting) Init() {
	r.Zmq.Initialize_Connector_Instance("dwx-zeromq", "localhost", "tcp", 32768, 32769, 32770, ";", map[string]interface{}{}, map[string]interface{}{}, true, 1000, 0.001, false)

}
