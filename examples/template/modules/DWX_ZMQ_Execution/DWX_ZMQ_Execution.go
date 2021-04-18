package DWX_ZMQ_Execution

import (
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

}
