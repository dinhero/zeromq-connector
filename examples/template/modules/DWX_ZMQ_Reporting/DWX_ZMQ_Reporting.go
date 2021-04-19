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
	"time"
	api_connect "zeromq-connector/api/ZeroMQ_Connector"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

type ZMQ_Reporting struct {
	Zmq api_connect.DWX_ZeroMQ_Connector
}

func (r *ZMQ_Reporting) Init() {
	r.Zmq.Initialize_Connector_Instance("dwx-zeromq", "localhost", "tcp", 32768, 32769, 32770, ";", nil, nil, true, 1000, 0.001, false)

}

func (r *ZMQ_Reporting) Get_open_trades_(_trader string, _delay float64, _wbreak float64) *dataframe.DataFrame {
	// # Reset data output
	r.Zmq.Set_response_(nil)
	// # Get open trades from MetaTrader
	r.Zmq.DWX_MTX_GET_ALL_OPEN_TRADES_()
	// # While loop start time reference
	_ws := time.Now().Unix()

	// # While data not received, sleep until timeout
	for !r.Zmq.Valid_response_("zmq") {

		time.Sleep(time.Duration(_delay))

		if float64((time.Now().Unix() - _ws)) > (_delay * _wbreak) {
			break
		}
	}

	// # If data received, return DataFrame
	if r.Zmq.Valid_response_("zmq") {
		_response := r.Zmq.Get_response_()

		value, exists := _response["_trades"]
		if exists && len(value.(map[string]interface{})) > 0 {

			dataframe_data := dataframe.NewSeriesString("data", nil)
			dataframe_index := dataframe.NewSeriesString("index", nil)
			df := dataframe.NewDataFrame(dataframe_data, dataframe_index)

			for key, value := range _response["_trades"].(map[string]interface{}) {
				df.Append(nil, value.(string), key)
			}

			df.AddSeries(dataframe.NewSeriesString("_comment", nil, _trader), nil) //return _df[_df['_comment'] == _trader]
			return df
		}
	}
	// Default
	return nil
}
