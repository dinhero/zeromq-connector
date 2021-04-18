package main

// # -*- coding: utf-8 -*-
// """
//     DWX_ZeroMQ_Connector_v2_0_1_RC8.py
//     --
//     @author: Darwinex Labs (www.darwinex.com)

//     Copyright (c) 2017-2019, Darwinex. All rights reserved.

//     Licensed under the BSD 3-Clause License, you may not use this file except
//     in compliance with the License.

//     You may obtain a copy of the License at:
//     https://opensource.org/licenses/BSD-3-Clause
// """
import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	//"runtime"
	zmq "github.com/pebbe/zmq4"
)

//Referred to a class in python
type DWX_ZeroMQ_Connector struct {
	_ClientID          string // Unique ID for this client
	_host              string //Host to connect to
	_protocol          string //Connection protocol
	_PUSH_PORT         int    //Port for Sending commands
	_PULL_PORT         int    //Port for Receiving responses
	_SUB_PORT          int    //Port for Subscribing for prices
	_delimiter         string
	_pulldata_handlers map[string]interface{} // List Handlers to process data received through PULL port.
	_subdata_handlers  map[string]interface{} // List Handlers to process data received through SUB port.
	_verbose           bool                   //String delimiter
	_poll_timeout      int                    //ZMQ Poller Timeout (ms)
	_sleep_delay       float32                // 1 ms for time.sleep()
	_monitor           bool

	//Params that are not initialized in constructor
	_ACTIVE             bool
	_ZMQ_CONTEXT        *zmq.Context
	_URL                string
	_PUSH_SOCKET        *zmq.Socket
	_PUSH_SOCKET_STATUS map[string]interface{}
	_PULL_SOCKET        *zmq.Socket
	_PULL_SOCKET_STATUS map[string]interface{}
	_SUB_SOCKET         *zmq.Socket
	_poller             *zmq.Poller
	_string_delimiter   string

	//Threads wait groups
	_MarketData_Thread   int
	_PUSH_Monitor_Thread int
	_PULL_Monitor_Thread int
	//Threads waitgroups
	_MarketData_Thread_WG   sync.WaitGroup
	_PUSH_Monitor_Thread_WG sync.WaitGroup
	_PULL_Monitor_Thread_WG sync.WaitGroup

	_Market_Data_DB map[string]interface{}
	_History_DB     map[string]interface{}

	temp_order_dict map[string]interface{}

	_thread_data_output map[string]interface{}
	_MONITOR_EVENT_MAP  map[int]string
}

func main() {
	fmt.Println("This is a line")
	// Get_Connector_Instance("", "", "", 0, 0, 0, "", map[string]interface{}{}, map[string]interface{}{}, true, 1000, 0.001, false)
}

func Get_Connector_Instance(ClientID string,
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
	monitor bool) *DWX_ZeroMQ_Connector {

	p := new(DWX_ZeroMQ_Connector)

	if ClientID == "" {
		ClientID = "dwx-zeromq"
	}
	if host == "" {
		host = "localhost"
	}
	if protocol == "" {
		host = "tcp"
	}
	if PUSH_PORT == 0 {
		PUSH_PORT = 32768
	}
	if PULL_PORT == 0 {
		PULL_PORT = 32769
	}
	if SUB_PORT == 0 {
		SUB_PORT = 32770
	}
	if delimiter == "" {
		delimiter = ";"
	}

	// Strategy Status (if this is False, ZeroMQ will not listen for data)
	p._ACTIVE = true

	// Client ID
	p._ClientID = ClientID

	// ZeroMQ Host
	p._host = host

	// Connection Protocol
	p._protocol = protocol

	// ZeroMQ Context
	p._ZMQ_CONTEXT, _ = zmq.NewContext()

	// TCP Connection URL Template
	p._URL = p._protocol + "://" + p._host + ":"

	// Handlers for received data (pull and sub ports)
	p._pulldata_handlers = pulldata_handlers
	p._subdata_handlers = subdata_handlers

	// Ports for PUSH, PULL and SUB sockets respectively
	p._PUSH_PORT = PUSH_PORT
	p._PULL_PORT = PULL_PORT
	p._SUB_PORT = SUB_PORT

	// Create Sockets
	p._PUSH_SOCKET, _ = p._ZMQ_CONTEXT.NewSocket(zmq.PUSH)
	p._PUSH_SOCKET.SetSndhwm(1)
	p._PUSH_SOCKET_STATUS["state"] = true
	p._PUSH_SOCKET_STATUS["latest_event"] = "N/A"

	p._PULL_SOCKET, _ = p._ZMQ_CONTEXT.NewSocket(zmq.PULL)
	p._PULL_SOCKET.SetRcvhwm(1)
	p._PULL_SOCKET_STATUS["state"] = true
	p._PULL_SOCKET_STATUS["latest_event"] = "N/A"

	p._SUB_SOCKET, _ = p._ZMQ_CONTEXT.NewSocket(zmq.SUB)

	// Bind PUSH Socket to send commands to MetaTrader
	p._PUSH_SOCKET.Connect(p._URL + strconv.Itoa(p._PUSH_PORT))
	fmt.Println("[INIT] Ready to send commands to METATRADER (PUSH): " + strconv.Itoa(p._PUSH_PORT))

	// Connect PULL Socket to receive command responses from MetaTrader
	p._PULL_SOCKET.Connect(p._URL + strconv.Itoa(p._PULL_PORT))
	fmt.Println("[INIT] Listening for responses from METATRADER (PULL): " + strconv.Itoa(p._PULL_PORT))

	// Connect SUB Socket to receive market data from MetaTrader
	fmt.Println("[INIT] Listening for market data from METATRADER (SUB): " + strconv.Itoa(p._SUB_PORT))
	p._SUB_SOCKET.Connect(p._URL + strconv.Itoa(p._SUB_PORT))

	// Initialize POLL set and register PULL and SUB sockets
	p._poller = zmq.NewPoller()
	p._poller.Add(p._PULL_SOCKET, zmq.POLLIN)
	p._poller.Add(p._SUB_SOCKET, zmq.POLLIN)

	// Start listening for responses to commands and new market data
	p._string_delimiter = delimiter

	// BID/ASK Market Data Subscription Threads ({SYMBOL: Thread})
	p._MarketData_Thread = 0

	// Socket Monitor Threads
	p._PUSH_Monitor_Thread = 0
	p._PULL_Monitor_Thread = 0

	// Market Data Dictionary by Symbol (holds tick data)
	p._Market_Data_DB = map[string]interface{}{} // {SYMBOL: {TIMESTAMP: (BID, ASK)}}

	// History Data Dictionary by Symbol (holds historic data of the last HIST request for each symbol)
	p._History_DB = map[string]interface{}{} // {SYMBOL_TF: [{'time': TIME, 'open': OPEN_PRICE, 'high': HIGH_PRICE,
	//               'low': LOW_PRICE, 'close': CLOSE_PRICE, 'tick_volume': TICK_VOLUME,
	//               'spread': SPREAD, 'real_volume': REAL_VOLUME}, ...]}

	// Temporary Order STRUCT for convenience wrappers later.
	p.temp_order_dict = p._generate_default_order_dict()

	// Thread returns the most recently received DATA block here
	p._thread_data_output = map[string]interface{}{}

	// Verbosity
	p._verbose = verbose

	// ZMQ Poller Timeout
	p._poll_timeout = poll_timeout

	// Global Sleep Delay
	p._sleep_delay = sleep_delay

	// Begin polling for PULL / SUB data
	// p._MarketData_Thread = Thread(target=p._DWX_ZMQ_Poll_Data_,
	//                                  args=(p._string_delimiter,
	//                                        p._poll_timeout,))
	//p._MarketData_Thread.daemon = True
	//p._MarketData_Thread.start()
	go p._DWX_ZMQ_Poll_Data_(p._string_delimiter, p._poll_timeout)
	// ###########################################
	// # Enable/Disable ZeroMQ Socket Monitoring #
	// ###########################################
	if monitor {

		//# ZeroMQ Monitor Event Map
		p._MONITOR_EVENT_MAP = map[int]string{}

		fmt.Println("[KERNEL] Retrieving ZeroMQ Monitor Event Names:")

		p._MONITOR_EVENT_MAP[32] = "EVENT_ACCEPTED"
		p._MONITOR_EVENT_MAP[64] = "EVENT_ACCEPT_FAILED"
		p._MONITOR_EVENT_MAP[65535] = "EVENT_ALL"
		p._MONITOR_EVENT_MAP[16] = "EVENT_BIND_FAILED"
		p._MONITOR_EVENT_MAP[128] = "EVENT_CLOSED"
		p._MONITOR_EVENT_MAP[256] = "EVENT_CLOSE_FAILED"
		p._MONITOR_EVENT_MAP[1] = "EVENT_CONNECTED"
		p._MONITOR_EVENT_MAP[2] = "EVENT_CONNECT_DELAYED"
		p._MONITOR_EVENT_MAP[4] = "EVENT_CONNECT_RETRIED"
		p._MONITOR_EVENT_MAP[512] = "EVENT_DISCONNECTED"
		p._MONITOR_EVENT_MAP[16384] = "EVENT_HANDSHAKE_FAILED_AUTH"
		p._MONITOR_EVENT_MAP[2048] = "EVENT_HANDSHAKE_FAILED_NO_DETAIL"
		p._MONITOR_EVENT_MAP[8192] = "EVENT_HANDSHAKE_FAILED_PROTOCOL"
		p._MONITOR_EVENT_MAP[4096] = "EVENT_HANDSHAKE_SUCCEEDED"
		p._MONITOR_EVENT_MAP[8] = "EVENT_LISTENING"
		p._MONITOR_EVENT_MAP[1024] = "EVENT_MONITOR_STOPPED"

		fmt.Println("[KERNEL] Socket Monitoring Config -> DONE!")

		//# Disable PUSH/PULL sockets and let MONITOR events control them.
		p._PUSH_SOCKET_STATUS["state"] = false
		p._PULL_SOCKET_STATUS["state"] = false

		//# PUSH
		go p._DWX_ZMQ_EVENT_MONITOR_("PUSH", p._PUSH_SOCKET)
		// self._PUSH_Monitor_Thread = Thread(target=self._DWX_ZMQ_EVENT_MONITOR_,
		// 								   args=("PUSH",
		// 										 self._PUSH_SOCKET.get_monitor_socket(),))

		// self._PUSH_Monitor_Thread.daemon = True
		// self._PUSH_Monitor_Thread.start()

		// //# PULL
		go p._DWX_ZMQ_EVENT_MONITOR_("PULL", p._PULL_SOCKET)
		// self._PULL_Monitor_Thread = Thread(target=self._DWX_ZMQ_EVENT_MONITOR_,
		// 								   args=("PULL",
		// 										 self._PULL_SOCKET.get_monitor_socket(),))

		// self._PULL_Monitor_Thread.daemon = True
		// self._PULL_Monitor_Thread.start()
	}
	return p
}

func (p *DWX_ZeroMQ_Connector) _DWX_ZMQ_SHUTDOWN_() {
	//# Set INACTIVE
	p._ACTIVE = false

	//   # Get all threads to shutdown
	// if p._MarketData_Thread is not None:
	//     p._MarketData_Thread.join()

	// if p._PUSH_Monitor_Thread is not None:
	//     p._PUSH_Monitor_Thread.join()

	// if p._PULL_Monitor_Thread is not None:
	//     p._PULL_Monitor_Thread.join()

	//    # Unregister sockets from Poller
	p._poller.RemoveBySocket(p._PULL_SOCKET)
	p._poller.RemoveBySocket(p._SUB_SOCKET)
	fmt.Println("\n++ [KERNEL] Sockets unregistered from ZMQ Poller()! ++")
	//   # Terminate context
	p._ZMQ_CONTEXT.Term()
	fmt.Println("\n++ [KERNEL] ZeroMQ Context Terminated.. shut down safely complete! :)")

	//##########################################################################
}

//##########################################################################

// """
// Set Status (to enable/disable strategy manually)
// """
func (p *DWX_ZeroMQ_Connector) _setStatus(_new_status bool) {

	p._ACTIVE = _new_status
	fmt.Printf("**[KERNEL] Setting Status to %t - Deactivating Threads.. please wait a bit.**", _new_status)
}

//##########################################################################
//"""
//  Function to send commands to MetaTrader (PUSH)
//  """
func (p *DWX_ZeroMQ_Connector) remote_send(_socket *zmq.Socket, _data string) {

	if p._PUSH_SOCKET_STATUS["state"] == "True" {
		_, err := _socket.Send(_data, zmq.DONTWAIT)
		if err != nil {
			// catch: zmq.Error()//zmq.error.Again:
			fmt.Println("Resource timeout.. please try again.")
			//     sleep(p._sleep_delay)
			time.Sleep(time.Millisecond * time.Duration(p._sleep_delay))
		}

	} else {
		fmt.Println("[KERNEL] NO HANDSHAKE ON PUSH SOCKET.. Cannot SEND data")
	}
}

func (p *DWX_ZeroMQ_Connector) _get_response_() map[string]interface{} {
	return p._thread_data_output
}

func (p *DWX_ZeroMQ_Connector) _set_response_(_resp map[string]interface{}) {
	p._thread_data_output = _resp
}

//Convenience functions to permit easy trading via underlying functions.

//OPEN ORDER
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_NEW_TRADE_(_order map[string]interface{}) {
	if _order == nil {
		_order = p._generate_default_order_dict()
	}
	//Execute
	p._DWX_MTX_SEND_COMMAND_(_order["_action"].(string), _order["_type"].(int),
		_order["_symbol"].(string), _order["_price"].(float32),
		_order["_SL"].(int), _order["_TP"].(int), _order["_comment"].(string),
		_order["_lots"].(float32), _order["_magic"].(int), _order["_ticket"].(int)) // (**_order)
}

//# MODIFY ORDER
//# _SL and _TP given in points. _price is only used for pending orders.
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_MODIFY_TRADE_BY_TICKET_(_ticket int, _SL int, _TP int, _price int) {

	p.temp_order_dict["_action"] = "MODIFY"
	p.temp_order_dict["_ticket"] = _ticket
	p.temp_order_dict["_SL"] = _SL
	p.temp_order_dict["_TP"] = _TP
	p.temp_order_dict["_price"] = _price

	//# Execute
	p._DWX_MTX_SEND_COMMAND_(p.temp_order_dict["_action"].(string), p.temp_order_dict["_type"].(int),
		p.temp_order_dict["_symbol"].(string), p.temp_order_dict["_price"].(float32),
		p.temp_order_dict["_SL"].(int), p.temp_order_dict["_TP"].(int), p.temp_order_dict["_comment"].(string),
		p.temp_order_dict["_lots"].(float32), p.temp_order_dict["_magic"].(int), p.temp_order_dict["_ticket"].(int))

	// except KeyError:
	// 	fmt.Printf("[ERROR] Order Ticket %d not found!",_ticket)
}

//# CLOSE ORDER
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_CLOSE_TRADE_BY_TICKET_(_ticket int) {

	p.temp_order_dict["_action"] = "CLOSE"
	p.temp_order_dict["_ticket"] = _ticket

	//            # Execute
	p._DWX_MTX_SEND_COMMAND_(p.temp_order_dict["_action"].(string), p.temp_order_dict["_type"].(int),
		p.temp_order_dict["_symbol"].(string), p.temp_order_dict["_price"].(float32),
		p.temp_order_dict["_SL"].(int), p.temp_order_dict["_TP"].(int), p.temp_order_dict["_comment"].(string),
		p.temp_order_dict["_lots"].(float32), p.temp_order_dict["_magic"].(int), p.temp_order_dict["_ticket"].(int))

	// except KeyError:
	//     print("[ERROR] Order Ticket {} not found!".format(_ticket))
}

//# CLOSE PARTIAL
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_CLOSE_PARTIAL_BY_TICKET_(_ticket int, _lots float32) {

	p.temp_order_dict["_action"] = "CLOSE_PARTIAL"
	p.temp_order_dict["_ticket"] = _ticket
	p.temp_order_dict["_lots"] = _lots

	//# Execute
	p._DWX_MTX_SEND_COMMAND_(p.temp_order_dict["_action"].(string), p.temp_order_dict["_type"].(int),
		p.temp_order_dict["_symbol"].(string), p.temp_order_dict["_price"].(float32),
		p.temp_order_dict["_SL"].(int), p.temp_order_dict["_TP"].(int), p.temp_order_dict["_comment"].(string),
		p.temp_order_dict["_lots"].(float32), p.temp_order_dict["_magic"].(int), p.temp_order_dict["_ticket"].(int))

	// except KeyError:
	//     print("[ERROR] Order Ticket {} not found!".format(_ticket))
}

//# CLOSE MAGIC
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_CLOSE_TRADES_BY_MAGIC_(_magic int) {

	p.temp_order_dict["_action"] = "CLOSE_MAGIC"
	p.temp_order_dict["_magic"] = _magic

	//          # Execute
	p._DWX_MTX_SEND_COMMAND_(p.temp_order_dict["_action"].(string), p.temp_order_dict["_type"].(int),
		p.temp_order_dict["_symbol"].(string), p.temp_order_dict["_price"].(float32),
		p.temp_order_dict["_SL"].(int), p.temp_order_dict["_TP"].(int), p.temp_order_dict["_comment"].(string),
		p.temp_order_dict["_lots"].(float32), p.temp_order_dict["_magic"].(int), p.temp_order_dict["_ticket"].(int))

	// except KeyError:
	//     pass
}

// # CLOSE ALL TRADES
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_CLOSE_ALL_TRADES_() {

	p.temp_order_dict["_action"] = "CLOSE_ALL"

	//    # Execute
	p._DWX_MTX_SEND_COMMAND_(p.temp_order_dict["_action"].(string), p.temp_order_dict["_type"].(int),
		p.temp_order_dict["_symbol"].(string), p.temp_order_dict["_price"].(float32),
		p.temp_order_dict["_SL"].(int), p.temp_order_dict["_TP"].(int), p.temp_order_dict["_comment"].(string),
		p.temp_order_dict["_lots"].(float32), p.temp_order_dict["_magic"].(int), p.temp_order_dict["_ticket"].(int))

	// except KeyError:
	//     pass
}

//# GET OPEN TRADES
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_GET_ALL_OPEN_TRADES_() {

	p.temp_order_dict["_action"] = "GET_OPEN_TRADES"

	// # Execute
	p._DWX_MTX_SEND_COMMAND_(p.temp_order_dict["_action"].(string), p.temp_order_dict["_type"].(int),
		p.temp_order_dict["_symbol"].(string), p.temp_order_dict["_price"].(float32),
		p.temp_order_dict["_SL"].(int), p.temp_order_dict["_TP"].(int), p.temp_order_dict["_comment"].(string),
		p.temp_order_dict["_lots"].(float32), p.temp_order_dict["_magic"].(int), p.temp_order_dict["_ticket"].(int))

	// except KeyError:
	// 	pass
}

// ##########################################################################

// """
// Function to construct messages for sending HIST commands to MetaTrader

// Because of broker GMT offset _end time might have to be modified.
// """
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_SEND_HIST_REQUEST_(_symbol string,
	_timeframe int,
	_start string,
	_end string) {
	//  #_end='2019.01.04 17:05:00'):

	_msg := "HIST;" + _symbol + ";" + strconv.Itoa(_timeframe) + ";" + _start + ";" + _end

	// # Send via PUSH Socket
	p.remote_send(p._PUSH_SOCKET, _msg)
}

// ##########################################################################
// """
// Function to construct messages for sending TRACK_PRICES commands to
// MetaTrader for real-time price updates
// """_symbols=['EURUSD']
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_SEND_TRACKPRICES_REQUEST_(_symbols []string) {
	_msg := "TRACK_PRICES"
	for _, s := range _symbols {
		_msg = _msg + ";" + s
	}
	//# Send via PUSH Socket
	p.remote_send(p._PUSH_SOCKET, _msg)
}

// ##########################################################################
// """
// Function to construct messages for sending TRACK_RATES commands to
// MetaTrader for OHLC
// """
//_instruments=[('EURUSD_M1','EURUSD',1)]
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_SEND_TRACKRATES_REQUEST_(_instruments [][]interface{}) {
	_msg := "TRACK_RATES"
	for _, i := range _instruments {
		_msg = _msg + ";" + i[1].(string) + ";" + i[2].(string)

	}
	// # Send via PUSH Socket
	p.remote_send(p._PUSH_SOCKET, _msg)
}

// ##########################################################################
// """
// Function to construct messages for sending Trade commands to MetaTrader
// """
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_SEND_COMMAND_(_action string, _type int,
	_symbol string, _price float32,
	_SL int, _TP int, _comment string,
	_lots float32, _magic int, _ticket int) {

	_msg := "TRADE;" + _action + ";" + strconv.Itoa(_type) + ";" + _symbol + ";" + fmt.Sprintf("%f", _price) + ";" + strconv.Itoa(_SL) + ";" + strconv.Itoa(_TP) + ";" + _comment + ";" + fmt.Sprintf("%f", _lots) + ";" + strconv.Itoa(_magic) + ";" + strconv.Itoa(_ticket)
	// .format("TRADE", _action, _type,
	// 	_symbol, _price,
	// 	_SL, _TP, _comment,
	// 	_lots, _magic,
	// 	_ticket)

	//# Send via PUSH Socket
	p.remote_send(p._PUSH_SOCKET, _msg)
}

// ##########################################################################

// """
//  compArray[0] = TRADE or DATA
//  compArray[1] = ACTION (e.g. OPEN, MODIFY, CLOSE)
//  compArray[2] = TYPE (e.g. OP_BUY, OP_SELL, etc - only used when ACTION=OPEN)

//  For compArray[0] == DATA, format is:
// 	 DATA|SYMBOL|TIMEFRAME|START_DATETIME|END_DATETIME

//  // ORDER TYPES:
//  // https://docs.mql4.com/constants/tradingconstants/orderproperties

//  // OP_BUY = 0
//  // OP_SELL = 1
//  // OP_BUYLIMIT = 2
//  // OP_SELLLIMIT = 3
//  // OP_BUYSTOP = 4
//  // OP_SELLSTOP = 5

//  compArray[3] = Symbol (e.g. EURUSD, etc.)
//  compArray[4] = Open/Close Price (ignored if ACTION = MODIFY)
//  compArray[5] = SL
//  compArray[6] = TP
//  compArray[7] = Trade Comment
//  compArray[8] = Lots
//  compArray[9] = Magic Number
//  compArray[10] = Ticket Number (MODIFY/CLOSE)
//  """
// # pass

//##########################################################################
func (p *DWX_ZeroMQ_Connector) _generate_default_order_dict() map[string]interface{} {
	return map[string]interface{}{"_action": "OPEN",
		"_type":    0,
		"_symbol":  "EURUSD",
		"_price":   0.0,
		"_SL":      500, // SL/TP in POINTS, not pips.
		"_TP":      500,
		"_comment": p._ClientID,
		"_lots":    0.01,
		"_magic":   123456,
		"_ticket":  0}
}

func (p *DWX_ZeroMQ_Connector) _DWX_ZMQ_Poll_Data_(string_delimiter string,
	poll_timeout int) { //=1000
	defer func() {
		if p._MarketData_Thread > 1 {
			p._MarketData_Thread--
		} else {
			p._MarketData_Thread = 0
		}
	}()
	p._MarketData_Thread++

	for p._ACTIVE {

		time.Sleep(time.Second * time.Duration(p._sleep_delay)) // poll timeout is in ms, sleep() is s.

		sockets, _ := p._poller.Poll(time.Duration(poll_timeout))
		for _, socket := range sockets {
			// Process response to commands sent to MetaTrader
			if (socket.Socket == p._PULL_SOCKET) && (socket.Events == zmq.POLLIN) {
				if p._PULL_SOCKET_STATUS["state"] == true {
					//msg = p._PULL_SOCKET.recv_string(zmq.DONTWAIT)
					msg := p.remote_recv(p._PULL_SOCKET)
					// If data is returned, store as pandas Series
					if msg != "" {
						_data := make(map[string]interface{})
						err := json.Unmarshal([]byte(msg), &_data)
						if err == nil {
							_dataValue, _dataExists := _data["action"]
							if _dataExists && _dataValue == "HIST" {
								_symbol := _data["_symbol"].(string)
								dataValue, dataExists := _data["_data"]
								if dataExists {
									_, _symbolExists := p._History_DB[_symbol]
									if !_symbolExists {
										p._History_DB[_symbol] = map[string]interface{}{}
									}
									p._History_DB[_symbol] = dataValue
								} else {
									fmt.Println("No data found. MT4 often needs multiple requests when accessing data of symbols without open charts.")
									fmt.Println("message: " + msg)
								}

							}
							//invokes data handlers on pull port
							for _, hnd := range p._pulldata_handlers {
								fmt.Println(hnd)
								hnd.onPullData(_data)
							}
							p._thread_data_output = _data
							if p._verbose {
								fmt.Println(_data)
							}
						} else {

							fmt.Println("Exception Type : " + err.Error())
						}
					}
				} else {
					fmt.Println("[KERNEL] NO HANDSHAKE on PULL SOCKET.. Cannot READ data.")
				}
			}
			if (socket.Socket == p._SUB_SOCKET) && (socket.Events == zmq.POLLIN) {
				msg, err := p._SUB_SOCKET.Recv(zmq.DONTWAIT)
				if err == nil {
					if msg != "" {
						_timestamp := time.Now().UTC()
						_data := strings.Split(msg, " ")
						if len(strings.Split(_data[1], string_delimiter)) == 2 {
							_bid_ask := strings.Split(_data[1], string_delimiter)
							if p._verbose {
								fmt.Println("[" + _data[0] + "] " + _timestamp.String() + " (" + _bid_ask[0] + "/" + _bid_ask[1] + ") BID/ASK")
							}

							// Update Market Data DB
							_, ok := p._Market_Data_DB[_data[0]]
							if !ok {
								p._Market_Data_DB[_data[0]] = map[string]interface{}{}
							}
							_bid_float, err := strconv.ParseFloat(_bid_ask[0], 2)
							_ask_float, err1 := strconv.ParseFloat(_bid_ask[1], 2)
							if err != nil || err1 != nil {
								return
							}
							p._Market_Data_DB[_data[0]].(map[string]interface{})[_timestamp.String()] = map[string]interface{}{"_bid": _bid_float, "_ask": _ask_float}
						} else if len(strings.Split(_data[1], string_delimiter)) == 8 {
							// _time, _open, _high, _low, _close, _tick_vol, _spread, _real_vol
							_data_splited := strings.Split(_data[1], string_delimiter)
							if p._verbose {
								fmt.Println("[" + _data[0] + "] " + _timestamp.String() + " (" + _data_splited[0] + "/" + _data_splited[1] + "/" + _data_splited[2] + "/" + _data_splited[3] + "/" + _data_splited[4] + "/" + _data_splited[5] + "/" + _data_splited[6] + "/" + _data_splited[7] + ") TIME/OPEN/HIGH/LOW/CLOSE/TICKVOL/SPREAD/VOLUME")
							}
							// # Update Market Rate DB
							_, ok := p._Market_Data_DB[_data[0]]
							if !ok {
								p._Market_Data_DB[_data[0]] = map[string]interface{}{}
							}
							_time_float, err := strconv.ParseFloat(_data_splited[0], 2)
							_open_float, err1 := strconv.ParseFloat(_data_splited[1], 2)
							_high_float, err2 := strconv.ParseFloat(_data_splited[2], 2)
							_low_float, err3 := strconv.ParseFloat(_data_splited[3], 2)
							_close_float, err4 := strconv.ParseFloat(_data_splited[4], 2)
							_tick_vol_float, err5 := strconv.ParseFloat(_data_splited[5], 2)
							_spread_float, err6 := strconv.ParseFloat(_data_splited[6], 2)
							_real_vol_float, err7 := strconv.ParseFloat(_data_splited[7], 2)
							if err != nil || err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil || err6 != nil || err7 != nil {
								return
							}
							p._Market_Data_DB[_data[0]].(map[string]interface{})[_timestamp.String()] = map[string]interface{}{"_time": _time_float, "_open": _open_float, "_high": _high_float, "_low": _low_float, "_close": _close_float, "_tick_vol": _tick_vol_float, "_spread": _spread_float, "_real_vol": _real_vol_float}

						}
						for _, hnd := range p._subdata_handlers {
							hnd.onSubData(msg)
						}
					}
				} else {
					//ERROR HANDLING
				}
			}
		}

		fmt.Println("++ [KERNEL] _DWX_ZMQ_Poll_Data_() Signing Out ++")
	}
}

func (p *DWX_ZeroMQ_Connector) _DWX_ZMQ_EVENT_MONITOR_(socket_name string, monitor_socket *zmq.Socket) {
	defer func() {
		if socket_name == "PUSH" {
			if p._PUSH_Monitor_Thread > 1 {
				p._PUSH_Monitor_Thread--
			} else {
				p._PUSH_Monitor_Thread = 0
			}
		} else if socket_name == "PULL" {
			if p._PULL_Monitor_Thread > 1 {
				p._PULL_Monitor_Thread--
			} else {
				p._PULL_Monitor_Thread = 0
			}
		}
	}()
	if socket_name == "PUSH" {
		p._PUSH_Monitor_Thread++
	} else if socket_name == "PULL" {
		p._PULL_Monitor_Thread++
	}
	for p._ACTIVE {
		time.Sleep(time.Second * time.Duration(p._sleep_delay))
		for true {
			var evt map[string]interface{}
			evt = recv_monitor_message(monitor_socket, zmq.DONTWAIT)
			evt["description"] = p._MONITOR_EVENT_MAP[evt["event"].(int)]

			//# print(f"\r[{socket_name} Socket] >> {evt['description']}", end='', flush=True)
			fmt.Println("[" + socket_name + "} Socket] >> {" + evt["description"].(string) + "}")

			if evt["event"] == 4096 {
				if socket_name == "PUSH" {
					p._PUSH_SOCKET_STATUS["state"] = true
					p._PUSH_SOCKET_STATUS["latest_event"] = "EVENT_HANDSHAKE_SUCCEEDED"
				} else if socket_name == "PULL" {
					p._PULL_SOCKET_STATUS["state"] = true
					p._PULL_SOCKET_STATUS["latest_event"] = "EVENT_HANDSHAKE_SUCCEEDED"
				}

				// # print(f"\n[{socket_name} Socket] >> ..ready for action!\n")
			} else {
				if socket_name == "PUSH" {
					p._PUSH_SOCKET_STATUS["state"] = false
					p._PUSH_SOCKET_STATUS["latest_event"] = evt["description"]

				} else if socket_name == "PULL" {
					p._PULL_SOCKET_STATUS["state"] = false
					p._PULL_SOCKET_STATUS["latest_event"] = evt["description"]
				}
			}
			if evt["event"] == zmq.EVENT_MONITOR_STOPPED {
				if socket_name == "PUSH" {
					monitor_socket = p._PUSH_SOCKET
				} else if socket_name == "PULL" {
					monitor_socket = p._PULL_SOCKET
				}
			}
			// except Exception as ex:
			//         _exstr = "Exception Type {0}. Args:\n{1!r}"
			//         _msg = _exstr.format(type(ex).__name__, ex.args)
			//         print(_msg)

		}
	}
	monitor_socket.Close()

	fmt.Println("++ [KERNEL] " + socket_name + " _DWX_ZMQ_EVENT_MONITOR_() Signing Out ++")
}
func (p *DWX_ZeroMQ_Connector) _valid_response_(_input string) bool {
	return true
	// //# Valid data types
	// _types = (dict,DataFrame)

	// //# If _input = 'zmq', assume self._zmq._thread_data_output
	// if isinstance(_input, str) and _input == 'zmq':
	//     return isinstance(self._get_response_(), _types)
	// else:
	//     return isinstance(_input, _types)

	// //# Default
	// return False
}
func (p *DWX_ZeroMQ_Connector) remote_recv(_socket *zmq.Socket) string {
	if p._PULL_SOCKET_STATUS["state"] == true {
		msg, err := _socket.Recv(zmq.DONTWAIT)
		if err == nil {
			return msg
		} else {
			fmt.Println("Resource timeout.. please try again")
			time.Sleep(time.Second * time.Duration(p._sleep_delay))
		}
	} else {
		fmt.Println("[KERNEL] NO HANDSHAKE on PULL SOCKET.. Cannot READ data.")
	}
	return ""
}

//##########################################################################
// """
// Function to subscribe to given Symbol's BID/ASK feed from MetaTrader
// """
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_SUBSCRIBE_MARKETDATA_(_symbol string,
	string_delimiter string) {

	//# Subscribe to SYMBOL first.
	p._SUB_SOCKET.Send(_symbol, zmq.Flag(zmq.SUB))
	fmt.Printf("[KERNEL] Subscribed to %s BID/ASK updates. See p._Market_Data_DB.", _symbol)
}

// """
//     Function to unsubscribe to given Symbol's BID/ASK feed from MetaTrader
//     """
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_UNSUBSCRIBE_MARKETDATA_(_symbol string) {

	p._SUB_SOCKET.Send(_symbol, zmq.Flag(zmq.XSUB))
	fmt.Println("[KERNEL] Unsubscribing from " + _symbol)
}

//     """
//     Function to unsubscribe from ALL MetaTrader Symbols
//     """
func (p *DWX_ZeroMQ_Connector) _DWX_MTX_UNSUBSCRIBE_ALL_MARKETDATA_REQUESTS_() {

	//# 31-07-2019 12:22 CEST
	for _symbol := range p._Market_Data_DB {
		p._DWX_MTX_UNSUBSCRIBE_MARKETDATA_(_symbol)
	}
}

//##########################################################################
func (p *DWX_ZeroMQ_Connector) _DWX_ZMQ_HEARTBEAT_() {
	p.remote_send(p._PUSH_SOCKET, "HEARTBEAT;")
}

//##########################################################################
func _DWX_ZMQ_CLEANUP_(_name string,
	_globals map[string]interface{},
	_locals map[string]interface{}) {

	fmt.Println("++ [KERNEL] Initializing ZeroMQ Cleanup.. if nothing appears below, no cleanup is necessary, otherwise please wait..")
	// try:
	//     _class = _globals[_name]
	//     _locals = list(_locals.items())

	//     for _func, _instance in _locals:
	//         if isinstance(_instance, _class):
	//             print(f'\n++ [KERNEL] Found & Destroying {_func} object before __init__()')
	//             eval(_func)._DWX_ZMQ_SHUTDOWN_()
	//             print('\n++ [KERNEL] Cleanup Complete -> OK to initialize DWX_ZeroMQ_Connector if NETSTAT diagnostics == True. ++\n')

	// except Exception as ex:

	//     _exstr = "Exception Type {0}. Args:\n{1!r}"
	//     _msg = _exstr.format(type(ex).__name__, ex.args)

	//     if 'KeyError' in _msg:
	//         print('\n++ [KERNEL] Cleanup Complete -> OK to initialize DWX_ZeroMQ_Connector. ++\n')
	//     else:
	//         print(_msg)
}

//##############################################################################
