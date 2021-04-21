package DWX_ZMQ_Strategy

// # -*- coding: utf-8 -*-
// """
//     DWX_ZMQ_Strategy.py
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
	execution "zeromq-connector/examples/template/modules/DWX_ZMQ_Execution"
	reporting "zeromq-connector/examples/template/modules/DWX_ZMQ_Reporting"
)

type ZMQ_Strategy struct {
	Zmq               api_connect.DWX_ZeroMQ_Connector
	Name              string                 //# Name
	Symbols           map[string]interface{} //# List of (Symbol,Lotsize) tuples
	Broker_gmt        int                    // # Darwinex GMT offset
	Pulldata_handlers map[string]interface{} //# Handlers to process data received through PULL port.
	Subdata_handlers  map[string]interface{} //# Handlers to process data received through SUB port.
	Verbose           bool                   // # Print ZeroMQ messages
	Execution         execution.ZMQ_Execution
	Reporting         reporting.ZMQ_Reporting
}

func (b *ZMQ_Strategy) Init(_name string,
	_symbols map[string]interface{},
	_broker_gmt int,
	_pulldata_handlers map[string]interface{},
	_subdata_handlers map[string]interface{},
	_verbose bool) {

	b.Name = _name
	b.Symbols = _symbols
	b.Broker_gmt = _broker_gmt

	// # Not entirely necessary here.
	b.Zmq.Initialize_Connector_Instance("dwx-zeromq", "localhost", "tcp", 32768, 32769, 32770, ";", _pulldata_handlers, _subdata_handlers, _verbose, 1000, 0.001, false)

	// Modules
	b.Execution.Init("dwx-zeromq", "localhost", "tcp", 32768, 32769, 32770, ";", _pulldata_handlers, _subdata_handlers, _verbose, 1000, 0.001, false)
	b.Reporting.Init("dwx-zeromq", "localhost", "tcp", 32768, 32769, 32770, ";", _pulldata_handlers, _subdata_handlers, _verbose, 1000, 0.001, false)

}

// ##########################################################################

func (b *ZMQ_Strategy) Run_() {

	// """
	// Enter strategy logic here
	// """
}

//##########################################################################
