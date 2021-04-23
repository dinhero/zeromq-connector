package ZeroMQ_Connector

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
	ClientID          string // Unique ID for this client
	Host              string //Host to connect to
	Protocol          string //Connection protocol
	PUSH_PORT         int    //Port for Sending commands
	PULL_PORT         int    //Port for Receiving responses
	SUB_PORT          int    //Port for Subscribing for prices
	Delimiter         string
	Pulldata_handlers map[string]interface{} // List Handlers to process data received through PULL port.
	Subdata_handlers  map[string]interface{} // List Handlers to process data received through SUB port.
	Verbose           bool                   //String delimiter
	Poll_timeout      int                    //ZMQ Poller Timeout (ms)
	Sleep_delay       float32                // 1 ms for time.sleep()
	Monitor           bool

	//Params that are not initialized in constructor
	ACTIVE             bool
	ZMQ_CONTEXT        *zmq.Context
	URL                string
	PUSH_SOCKET        *zmq.Socket
	PUSH_SOCKET_STATUS map[string]interface{}
	PULL_SOCKET        *zmq.Socket
	PULL_SOCKET_STATUS map[string]interface{}
	SUB_SOCKET         *zmq.Socket
	Poller             *zmq.Poller
	String_delimiter   string

	//Threads wait groups
	MarketData_Thread   int
	PUSH_Monitor_Thread int
	PULL_Monitor_Thread int
	//Threads waitgroups
	MarketData_Thread_WG   sync.WaitGroup
	PUSH_Monitor_Thread_WG sync.WaitGroup
	PULL_Monitor_Thread_WG sync.WaitGroup

	Market_Data_DB map[string]interface{}
	History_DB     map[string]interface{}

	Temp_order_dict map[string]interface{}

	Thread_data_output map[string]interface{}
	MONITOR_EVENT_MAP  map[int]string
}

func (p *DWX_ZeroMQ_Connector) Initialize_Connector_Instance(ClientID string,
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

	// p := new(DWX_ZeroMQ_Connector)

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
	p.ACTIVE = true

	// Client ID
	p.ClientID = ClientID

	// ZeroMQ Host
	p.Host = host

	// Connection Protocol
	p.Protocol = protocol

	// ZeroMQ Context
	p.ZMQ_CONTEXT, _ = zmq.NewContext()

	// TCP Connection URL Template
	p.URL = p.Protocol + "://" + p.Host + ":"

	// Handlers for received data (pull and sub ports)
	p.Pulldata_handlers = pulldata_handlers
	p.Subdata_handlers = subdata_handlers

	// Ports for PUSH, PULL and SUB sockets respectively
	p.PUSH_PORT = PUSH_PORT
	p.PULL_PORT = PULL_PORT
	p.SUB_PORT = SUB_PORT

	// Create Sockets
	p.PUSH_SOCKET, _ = p.ZMQ_CONTEXT.NewSocket(zmq.Type(zmq.PUSH))
	p.PUSH_SOCKET.SetSndhwm(1)
	p.PUSH_SOCKET_STATUS["state"] = true
	p.PUSH_SOCKET_STATUS["latest_event"] = "N/A"

	p.PULL_SOCKET, _ = p.ZMQ_CONTEXT.NewSocket(zmq.Type(zmq.PULL))
	p.PULL_SOCKET.SetRcvhwm(1)
	p.PULL_SOCKET_STATUS["state"] = true
	p.PULL_SOCKET_STATUS["latest_event"] = "N/A"

	p.SUB_SOCKET, _ = p.ZMQ_CONTEXT.NewSocket(zmq.Type(zmq.SUB))

	// Bind PUSH Socket to send commands to MetaTrader
	p.PUSH_SOCKET.Connect(p.URL + strconv.Itoa(p.PUSH_PORT))
	fmt.Println("[INIT] Ready to send commands to METATRADER (PUSH): " + strconv.Itoa(p.PUSH_PORT))

	// Connect PULL Socket to receive command responses from MetaTrader
	p.PULL_SOCKET.Connect(p.URL + strconv.Itoa(p.PULL_PORT))
	fmt.Println("[INIT] Listening for responses from METATRADER (PULL): " + strconv.Itoa(p.PULL_PORT))

	// Connect SUB Socket to receive market data from MetaTrader
	fmt.Println("[INIT] Listening for market data from METATRADER (SUB): " + strconv.Itoa(p.SUB_PORT))
	p.SUB_SOCKET.Connect(p.URL + strconv.Itoa(p.SUB_PORT))

	// Initialize POLL set and register PULL and SUB sockets
	p.Poller = zmq.NewPoller()
	p.Poller.Add(p.PULL_SOCKET, zmq.State(zmq.POLLIN))
	p.Poller.Add(p.SUB_SOCKET, zmq.State(zmq.POLLIN))

	// Start listening for responses to commands and new market data
	p.String_delimiter = delimiter

	// BID/ASK Market Data Subscription Threads ({SYMBOL: Thread})
	p.MarketData_Thread = 0

	// Socket Monitor Threads
	p.PUSH_Monitor_Thread = 0
	p.PULL_Monitor_Thread = 0

	// Market Data Dictionary by Symbol (holds tick data)
	p.Market_Data_DB = map[string]interface{}{} // {SYMBOL: {TIMESTAMP: (BID, ASK)}}

	// History Data Dictionary by Symbol (holds historic data of the last HIST request for each symbol)
	p.History_DB = map[string]interface{}{} // {SYMBOL_TF: [{'time': TIME, 'open': OPEN_PRICE, 'high': HIGH_PRICE,
	//               'low': LOW_PRICE, 'close': CLOSE_PRICE, 'tick_volume': TICK_VOLUME,
	//               'spread': SPREAD, 'real_volume': REAL_VOLUME}, ...]}

	// Temporary Order STRUCT for convenience wrappers later.
	p.Temp_order_dict = p.Generate_default_order_dict()

	// Thread returns the most recently received DATA block here
	p.Thread_data_output = nil

	// Verbosity
	p.Verbose = verbose

	// ZMQ Poller Timeout
	p.Poll_timeout = poll_timeout

	// Global Sleep Delay
	p.Sleep_delay = sleep_delay

	// Begin polling for PULL / SUB data
	// p.MarketData_Thread = Thread(target=p._DWX_ZMQ_Poll_Data_,
	//                                  args=(p._string_delimiter,
	//                                        p._poll_timeout,))
	//p.MarketData_Thread.daemon = True
	//p.MarketData_Thread.start()
	go p.DWX_ZMQ_Poll_Data_(p.String_delimiter, p.Poll_timeout)
	// ###########################################
	// # Enable/Disable ZeroMQ Socket Monitoring #
	// ###########################################
	if monitor {

		//# ZeroMQ Monitor Event Map
		p.MONITOR_EVENT_MAP = map[int]string{}

		fmt.Println("[KERNEL] Retrieving ZeroMQ Monitor Event Names:")

		p.MONITOR_EVENT_MAP[32] = "EVENT_ACCEPTED"
		p.MONITOR_EVENT_MAP[64] = "EVENT_ACCEPT_FAILED"
		p.MONITOR_EVENT_MAP[65535] = "EVENT_ALL"
		p.MONITOR_EVENT_MAP[16] = "EVENT_BIND_FAILED"
		p.MONITOR_EVENT_MAP[128] = "EVENT_CLOSED"
		p.MONITOR_EVENT_MAP[256] = "EVENT_CLOSE_FAILED"
		p.MONITOR_EVENT_MAP[1] = "EVENT_CONNECTED"
		p.MONITOR_EVENT_MAP[2] = "EVENT_CONNECT_DELAYED"
		p.MONITOR_EVENT_MAP[4] = "EVENT_CONNECT_RETRIED"
		p.MONITOR_EVENT_MAP[512] = "EVENT_DISCONNECTED"
		p.MONITOR_EVENT_MAP[16384] = "EVENT_HANDSHAKE_FAILED_AUTH"
		p.MONITOR_EVENT_MAP[2048] = "EVENT_HANDSHAKE_FAILED_NO_DETAIL"
		p.MONITOR_EVENT_MAP[8192] = "EVENT_HANDSHAKE_FAILED_PROTOCOL"
		p.MONITOR_EVENT_MAP[4096] = "EVENT_HANDSHAKE_SUCCEEDED"
		p.MONITOR_EVENT_MAP[8] = "EVENT_LISTENING"
		p.MONITOR_EVENT_MAP[1024] = "EVENT_MONITOR_STOPPED"

		fmt.Println("[KERNEL] Socket Monitoring Config -> DONE!")

		//# Disable PUSH/PULL sockets and let MONITOR events control them.
		p.PUSH_SOCKET_STATUS["state"] = false
		p.PULL_SOCKET_STATUS["state"] = false

		//# PUSH
		go p.DWX_ZMQ_EVENT_MONITOR_("PUSH", p.PUSH_SOCKET)
		// self._PUSH_Monitor_Thread = Thread(target=self._DWX_ZMQ_EVENT_MONITOR_,
		// 								   args=("PUSH",
		// 										 self._PUSH_SOCKET.get_monitor_socket(),))

		// self._PUSH_Monitor_Thread.daemon = True
		// self._PUSH_Monitor_Thread.start()

		// //# PULL
		go p.DWX_ZMQ_EVENT_MONITOR_("PULL", p.PULL_SOCKET)
		// self._PULL_Monitor_Thread = Thread(target=self._DWX_ZMQ_EVENT_MONITOR_,
		// 								   args=("PULL",
		// 										 self._PULL_SOCKET.get_monitor_socket(),))

		// self._PULL_Monitor_Thread.daemon = True
		// self._PULL_Monitor_Thread.start()
	}
	return p
}

func (p *DWX_ZeroMQ_Connector) DWX_ZMQ_SHUTDOWN_() {
	//# Set INACTIVE
	p.ACTIVE = false

	//   # Get all threads to shutdown
	// if p.MarketData_Thread is not None:
	//     p.MarketData_Thread.join()

	// if p.PUSH_Monitor_Thread is not None:
	//     p.PUSH_Monitor_Thread.join()

	// if p.PULL_Monitor_Thread is not None:
	//     p.PULL_Monitor_Thread.join()

	//    # Unregister sockets from Poller
	p.Poller.RemoveBySocket(p.PULL_SOCKET)
	p.Poller.RemoveBySocket(p.SUB_SOCKET)
	fmt.Println("++ [KERNEL] Sockets unregistered from ZMQ Poller()! ++")
	//   # Terminate context
	p.ZMQ_CONTEXT.Term()
	fmt.Println("++ [KERNEL] ZeroMQ Context Terminated.. shut down safely complete! :)")

	//##########################################################################
}

//##########################################################################

// """
// Set Status (to enable/disable strategy manually)
// """
func (p *DWX_ZeroMQ_Connector) SetStatus(_new_status bool) {

	p.ACTIVE = _new_status
	fmt.Printf("**[KERNEL] Setting Status to %t - Deactivating Threads.. please wait a bit.**", _new_status)
}

//##########################################################################
//"""
//  Function to send commands to MetaTrader (PUSH)
//  """
func (p *DWX_ZeroMQ_Connector) Remote_send(_socket *zmq.Socket, _data string) {

	if p.PUSH_SOCKET_STATUS["state"] == "True" {
		_, err := _socket.Send(_data, zmq.Flag(zmq.DONTWAIT))
		if err != nil {
			// catch: zmq.Error()//zmq.error.Again:
			fmt.Println("Resource timeout.. please try again.")
			//     sleep(p.Sleep_delay)
			time.Sleep(time.Millisecond * time.Duration(p.Sleep_delay))
		}

	} else {
		fmt.Println("[KERNEL] NO HANDSHAKE ON PUSH SOCKET.. Cannot SEND data")
	}
}

func (p *DWX_ZeroMQ_Connector) Get_response_() map[string]interface{} {
	return p.Thread_data_output
}

func (p *DWX_ZeroMQ_Connector) Set_response_(_resp map[string]interface{}) {
	p.Thread_data_output = _resp
}

//Convenience functions to permit easy trading via underlying functions.

//OPEN ORDER
func (p *DWX_ZeroMQ_Connector) DWX_MTX_NEW_TRADE_(_order map[string]interface{}) {
	if _order == nil {
		_order = p.Generate_default_order_dict()
	}
	//Execute
	p.DWX_MTX_SEND_COMMAND_(_order["_action"].(string), _order["_type"].(int),
		_order["_symbol"].(string), _order["_price"].(float32),
		_order["_SL"].(int), _order["_TP"].(int), _order["_comment"].(string),
		_order["_lots"].(float32), _order["_magic"].(int), _order["_ticket"].(int)) // (**_order)
}

//# MODIFY ORDER
//# _SL and _TP given in points. _price is only used for pending orders.
func (p *DWX_ZeroMQ_Connector) DWX_MTX_MODIFY_TRADE_BY_TICKET_(_ticket int, _SL int, _TP int, _price int) {

	p.Temp_order_dict["_action"] = "MODIFY"
	p.Temp_order_dict["_ticket"] = _ticket
	p.Temp_order_dict["_SL"] = _SL
	p.Temp_order_dict["_TP"] = _TP
	p.Temp_order_dict["_price"] = _price

	//# Execute
	p.DWX_MTX_SEND_COMMAND_(p.Temp_order_dict["_action"].(string), p.Temp_order_dict["_type"].(int),
		p.Temp_order_dict["_symbol"].(string), p.Temp_order_dict["_price"].(float32),
		p.Temp_order_dict["_SL"].(int), p.Temp_order_dict["_TP"].(int), p.Temp_order_dict["_comment"].(string),
		p.Temp_order_dict["_lots"].(float32), p.Temp_order_dict["_magic"].(int), p.Temp_order_dict["_ticket"].(int))

	// except KeyError:
	// 	fmt.Printf("[ERROR] Order Ticket %d not found!",_ticket)
}

//# CLOSE ORDER
func (p *DWX_ZeroMQ_Connector) DWX_MTX_CLOSE_TRADE_BY_TICKET_(_ticket int) {

	p.Temp_order_dict["_action"] = "CLOSE"
	p.Temp_order_dict["_ticket"] = _ticket

	//            # Execute
	p.DWX_MTX_SEND_COMMAND_(p.Temp_order_dict["_action"].(string), p.Temp_order_dict["_type"].(int),
		p.Temp_order_dict["_symbol"].(string), p.Temp_order_dict["_price"].(float32),
		p.Temp_order_dict["_SL"].(int), p.Temp_order_dict["_TP"].(int), p.Temp_order_dict["_comment"].(string),
		p.Temp_order_dict["_lots"].(float32), p.Temp_order_dict["_magic"].(int), p.Temp_order_dict["_ticket"].(int))

	// except KeyError:
	//     print("[ERROR] Order Ticket {} not found!".format(_ticket))
}

//# CLOSE PARTIAL
func (p *DWX_ZeroMQ_Connector) DWX_MTX_CLOSE_PARTIAL_BY_TICKET_(_ticket int, _lots float32) {

	p.Temp_order_dict["_action"] = "CLOSE_PARTIAL"
	p.Temp_order_dict["_ticket"] = _ticket
	p.Temp_order_dict["_lots"] = _lots

	//# Execute
	p.DWX_MTX_SEND_COMMAND_(p.Temp_order_dict["_action"].(string), p.Temp_order_dict["_type"].(int),
		p.Temp_order_dict["_symbol"].(string), p.Temp_order_dict["_price"].(float32),
		p.Temp_order_dict["_SL"].(int), p.Temp_order_dict["_TP"].(int), p.Temp_order_dict["_comment"].(string),
		p.Temp_order_dict["_lots"].(float32), p.Temp_order_dict["_magic"].(int), p.Temp_order_dict["_ticket"].(int))

	// except KeyError:
	//     print("[ERROR] Order Ticket {} not found!".format(_ticket))
}

//# CLOSE MAGIC
func (p *DWX_ZeroMQ_Connector) DWX_MTX_CLOSE_TRADES_BY_MAGIC_(_magic int) {

	p.Temp_order_dict["_action"] = "CLOSE_MAGIC"
	p.Temp_order_dict["_magic"] = _magic

	//          # Execute
	p.DWX_MTX_SEND_COMMAND_(p.Temp_order_dict["_action"].(string), p.Temp_order_dict["_type"].(int),
		p.Temp_order_dict["_symbol"].(string), p.Temp_order_dict["_price"].(float32),
		p.Temp_order_dict["_SL"].(int), p.Temp_order_dict["_TP"].(int), p.Temp_order_dict["_comment"].(string),
		p.Temp_order_dict["_lots"].(float32), p.Temp_order_dict["_magic"].(int), p.Temp_order_dict["_ticket"].(int))

	// except KeyError:
	//     pass
}

// # CLOSE ALL TRADES
func (p *DWX_ZeroMQ_Connector) DWX_MTX_CLOSE_ALL_TRADES_() {

	p.Temp_order_dict["_action"] = "CLOSE_ALL"

	//    # Execute
	p.DWX_MTX_SEND_COMMAND_(p.Temp_order_dict["_action"].(string), p.Temp_order_dict["_type"].(int),
		p.Temp_order_dict["_symbol"].(string), p.Temp_order_dict["_price"].(float32),
		p.Temp_order_dict["_SL"].(int), p.Temp_order_dict["_TP"].(int), p.Temp_order_dict["_comment"].(string),
		p.Temp_order_dict["_lots"].(float32), p.Temp_order_dict["_magic"].(int), p.Temp_order_dict["_ticket"].(int))

	// except KeyError:
	//     pass
}

//# GET OPEN TRADES
func (p *DWX_ZeroMQ_Connector) DWX_MTX_GET_ALL_OPEN_TRADES_() {

	p.Temp_order_dict["_action"] = "GET_OPEN_TRADES"

	// # Execute
	p.DWX_MTX_SEND_COMMAND_(p.Temp_order_dict["_action"].(string), p.Temp_order_dict["_type"].(int),
		p.Temp_order_dict["_symbol"].(string), p.Temp_order_dict["_price"].(float32),
		p.Temp_order_dict["_SL"].(int), p.Temp_order_dict["_TP"].(int), p.Temp_order_dict["_comment"].(string),
		p.Temp_order_dict["_lots"].(float32), p.Temp_order_dict["_magic"].(int), p.Temp_order_dict["_ticket"].(int))

	// except KeyError:
	// 	pass
}

// ##########################################################################

// """
// Function to construct messages for sending HIST commands to MetaTrader

// Because of broker GMT offset _end time might have to be modified.
// """
func (p *DWX_ZeroMQ_Connector) DWX_MTX_SEND_HIST_REQUEST_(_symbol string,
	_timeframe int,
	_start string,
	_end string) {
	//  #_end='2019.01.04 17:05:00'):

	_msg := "HIST;" + _symbol + ";" + strconv.Itoa(_timeframe) + ";" + _start + ";" + _end

	// # Send via PUSH Socket
	p.Remote_send(p.PUSH_SOCKET, _msg)
}

// ##########################################################################
// """
// Function to construct messages for sending TRACK_PRICES commands to
// MetaTrader for real-time price updates
// """_symbols=['EURUSD']
func (p *DWX_ZeroMQ_Connector) DWX_MTX_SEND_TRACKPRICES_REQUEST_(_symbols map[string]interface{}) {
	_msg := "TRACK_PRICES"
	for s := range _symbols {
		_msg = _msg + ";" + s
	}
	//# Send via PUSH Socket
	p.Remote_send(p.PUSH_SOCKET, _msg)
}

// ##########################################################################
// """
// Function to construct messages for sending TRACK_RATES commands to
// MetaTrader for OHLC
// """
//_instruments=[('EURUSD_M1','EURUSD',1)]
func (p *DWX_ZeroMQ_Connector) DWX_MTX_SEND_TRACKRATES_REQUEST_(_instruments [][]interface{}) {
	_msg := "TRACK_RATES"
	for _, i := range _instruments {
		_msg = _msg + ";" + i[1].(string) + ";" + i[2].(string)

	}
	// # Send via PUSH Socket
	p.Remote_send(p.PUSH_SOCKET, _msg)
}

// ##########################################################################
// """
// Function to construct messages for sending Trade commands to MetaTrader
// """
func (p *DWX_ZeroMQ_Connector) DWX_MTX_SEND_COMMAND_(_action string, _type int,
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
	p.Remote_send(p.PUSH_SOCKET, _msg)
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
func (p *DWX_ZeroMQ_Connector) Generate_default_order_dict() map[string]interface{} {
	return map[string]interface{}{"_action": "OPEN",
		"_type":    0,
		"_symbol":  "EURUSD",
		"_price":   0.0,
		"_SL":      500, // SL/TP in POINTS, not pips.
		"_TP":      500,
		"_comment": p.ClientID,
		"_lots":    0.01,
		"_magic":   123456,
		"_ticket":  0}
}

func (p *DWX_ZeroMQ_Connector) DWX_ZMQ_Poll_Data_(string_delimiter string,
	poll_timeout int) { //=1000
	defer func() {
		if p.MarketData_Thread > 1 {
			p.MarketData_Thread--
		} else {
			p.MarketData_Thread = 0
		}
	}()
	p.MarketData_Thread++

	for p.ACTIVE {

		time.Sleep(time.Second * time.Duration(p.Sleep_delay)) // poll timeout is in ms, sleep() is s.

		sockets, _ := p.Poller.Poll(time.Duration(poll_timeout))
		for _, socket := range sockets {
			// Process response to commands sent to MetaTrader
			if (socket.Socket == p.PULL_SOCKET) && (socket.Events == zmq.State(zmq.POLLIN)) {
				if p.PULL_SOCKET_STATUS["state"] == true {
					//msg = p.PULL_SOCKET.recv_string(zmq.DONTWAIT)
					msg := p.Remote_recv(p.PULL_SOCKET)
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
									_, _symbolExists := p.History_DB[_symbol]
									if !_symbolExists {
										p.History_DB[_symbol] = map[string]interface{}{}
									}
									p.History_DB[_symbol] = dataValue
								} else {
									fmt.Println("No data found. MT4 often needs multiple requests when accessing data of symbols without open charts.")
									fmt.Println("message: " + msg)
								}

							}
							//invokes data handlers on pull port
							for index, hnd := range p.Pulldata_handlers {
								//fmt.Println(hnd)
								if index == "OnPullData" {
									_data_string, _ := json.Marshal(_data)
									hnd.(func(string))(string(_data_string)) //.onPullData(_data)
								}
							}
							p.Thread_data_output = _data
							if p.Verbose {
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
			if (socket.Socket == p.SUB_SOCKET) && (socket.Events == zmq.State(zmq.POLLIN)) {
				msg, err := p.SUB_SOCKET.Recv(zmq.Flag(zmq.DONTWAIT))
				if err == nil {
					if msg != "" {
						_timestamp := time.Now().UTC()
						_data := strings.Split(msg, " ")
						if len(strings.Split(_data[1], string_delimiter)) == 2 {
							_bid_ask := strings.Split(_data[1], string_delimiter)
							if p.Verbose {
								fmt.Println("[" + _data[0] + "] " + _timestamp.String() + " (" + _bid_ask[0] + "/" + _bid_ask[1] + ") BID/ASK")
							}

							// Update Market Data DB
							_, ok := p.Market_Data_DB[_data[0]]
							if !ok {
								p.Market_Data_DB[_data[0]] = map[string]interface{}{}
							}
							_bid_float, err := strconv.ParseFloat(_bid_ask[0], 2)
							_ask_float, err1 := strconv.ParseFloat(_bid_ask[1], 2)
							if err != nil || err1 != nil {
								return
							}
							p.Market_Data_DB[_data[0]].(map[string]interface{})[_timestamp.String()] = map[string]interface{}{"_bid": _bid_float, "_ask": _ask_float}
						} else if len(strings.Split(_data[1], string_delimiter)) == 8 {
							// _time, _open, _high, _low, _close, _tick_vol, _spread, _real_vol
							_data_splited := strings.Split(_data[1], string_delimiter)
							if p.Verbose {
								fmt.Println("[" + _data[0] + "] " + _timestamp.String() + " (" + _data_splited[0] + "/" + _data_splited[1] + "/" + _data_splited[2] + "/" + _data_splited[3] + "/" + _data_splited[4] + "/" + _data_splited[5] + "/" + _data_splited[6] + "/" + _data_splited[7] + ") TIME/OPEN/HIGH/LOW/CLOSE/TICKVOL/SPREAD/VOLUME")
							}
							// # Update Market Rate DB
							_, ok := p.Market_Data_DB[_data[0]]
							if !ok {
								p.Market_Data_DB[_data[0]] = map[string]interface{}{}
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
							p.Market_Data_DB[_data[0]].(map[string]interface{})[_timestamp.String()] = map[string]interface{}{"_time": _time_float, "_open": _open_float, "_high": _high_float, "_low": _low_float, "_close": _close_float, "_tick_vol": _tick_vol_float, "_spread": _spread_float, "_real_vol": _real_vol_float}

						}
						for index, hnd := range p.Subdata_handlers {
							if index == "OnSubData" {
								hnd.(func(string))(msg) //onSubData(msg)
							}
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

func (p *DWX_ZeroMQ_Connector) DWX_ZMQ_EVENT_MONITOR_(socket_name string, monitor_socket *zmq.Socket) {
	defer func() {
		if socket_name == "PUSH" {
			if p.PUSH_Monitor_Thread > 1 {
				p.PUSH_Monitor_Thread--
			} else {
				p.PUSH_Monitor_Thread = 0
			}
		} else if socket_name == "PULL" {
			if p.PULL_Monitor_Thread > 1 {
				p.PULL_Monitor_Thread--
			} else {
				p.PULL_Monitor_Thread = 0
			}
		}
	}()
	if socket_name == "PUSH" {
		p.PUSH_Monitor_Thread++
	} else if socket_name == "PULL" {
		p.PULL_Monitor_Thread++
	}
	for p.ACTIVE {
		time.Sleep(time.Second * time.Duration(p.Sleep_delay))
		for true {
			var evt map[string]interface{}
			_, _, evtValue, _ := monitor_socket.RecvEvent(zmq.Flag(zmq.DONTWAIT))
			//evt = recv_monitor_message(monitor_socket, zmq.DONTWAIT)
			evt["description"] = p.MONITOR_EVENT_MAP[evtValue]
			evt["event"] = evtValue

			//# print(f"\r[{socket_name} Socket] >> {evt['description']}", end='', flush=True)
			fmt.Println("[" + socket_name + "} Socket] >> {" + evt["description"].(string) + "}")

			if evt["event"] == 4096 {
				if socket_name == "PUSH" {
					p.PUSH_SOCKET_STATUS["state"] = true
					p.PUSH_SOCKET_STATUS["latest_event"] = "EVENT_HANDSHAKE_SUCCEEDED"
				} else if socket_name == "PULL" {
					p.PULL_SOCKET_STATUS["state"] = true
					p.PULL_SOCKET_STATUS["latest_event"] = "EVENT_HANDSHAKE_SUCCEEDED"
				}

				// # print(f"\n[{socket_name} Socket] >> ..ready for action!\n")
			} else {
				if socket_name == "PUSH" {
					p.PUSH_SOCKET_STATUS["state"] = false
					p.PUSH_SOCKET_STATUS["latest_event"] = evt["description"]

				} else if socket_name == "PULL" {
					p.PULL_SOCKET_STATUS["state"] = false
					p.PULL_SOCKET_STATUS["latest_event"] = evt["description"]
				}
			}
			if evt["event"] == zmq.EVENT_MONITOR_STOPPED {
				if socket_name == "PUSH" {
					monitor_socket = p.PUSH_SOCKET
				} else if socket_name == "PULL" {
					monitor_socket = p.PULL_SOCKET
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
func (p *DWX_ZeroMQ_Connector) Valid_response_(_input map[string]interface{}) bool {

	if _input["zmq"] == "zmq" {
		return isInstance(p.Get_response_())
	} else {
		return isInstance(_input)
	}
}
func isInstance(input map[string]interface{}) bool {
	return true
}

// func isinstance() bool {
// 	array := []string{"bool",
// 		"int",
// 		"int8",
// 		"int16",
// 		"int32",
// 		"int64",
// 		"uint",
// 		"uint8",
// 		"uint16",
// 		"uint32",
// 		"uint64",
// 		"uintptr",
// 		"float32",
// 		"float64",
// 		"complex64",
// 		"complex128",
// 		"array",
// 		"chan",
// 		"func",
// 		"interface",
// 		"map",
// 		"ptr",
// 		"slice",
// 		"string",
// 		"struct",
// 		"unsafePointer"}
// 	return false
// }
func (p *DWX_ZeroMQ_Connector) Remote_recv(_socket *zmq.Socket) string {
	if p.PULL_SOCKET_STATUS["state"] == true {
		msg, err := _socket.Recv(zmq.Flag(zmq.DONTWAIT))
		if err == nil {
			return msg
		} else {
			fmt.Println("Resource timeout.. please try again")
			time.Sleep(time.Second * time.Duration(p.Sleep_delay))
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
func (p *DWX_ZeroMQ_Connector) DWX_MTX_SUBSCRIBE_MARKETDATA_(_symbol string,
	string_delimiter string) {

	//# Subscribe to SYMBOL first.
	p.SUB_SOCKET.Send(_symbol, zmq.Flag(zmq.SUB))
	fmt.Printf("[KERNEL] Subscribed to %s BID/ASK updates. See p.Market_Data_DB.", _symbol)
}

// """
//     Function to unsubscribe to given Symbol's BID/ASK feed from MetaTrader
//     """
func (p *DWX_ZeroMQ_Connector) DWX_MTX_UNSUBSCRIBE_MARKETDATA_(_symbol string) {

	p.SUB_SOCKET.Send(_symbol, zmq.Flag(zmq.XSUB))
	fmt.Println("[KERNEL] Unsubscribing from " + _symbol)
}

//     """
//     Function to unsubscribe from ALL MetaTrader Symbols
//     """
func (p *DWX_ZeroMQ_Connector) DWX_MTX_UNSUBSCRIBE_ALL_MARKETDATA_REQUESTS_() {

	//# 31-07-2019 12:22 CEST
	for _symbol := range p.Market_Data_DB {
		p.DWX_MTX_UNSUBSCRIBE_MARKETDATA_(_symbol)
	}
}

//##########################################################################
func (p *DWX_ZeroMQ_Connector) DWX_ZMQ_HEARTBEAT_() {
	p.Remote_send(p.PUSH_SOCKET, "HEARTBEAT;")
}

//##########################################################################
func DWX_ZMQ_CLEANUP_(_name string,
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
