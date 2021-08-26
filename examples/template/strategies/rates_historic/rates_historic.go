package rates_historic

// #!/usr/bin/env python
// # -*- coding: utf-8 -*-

// """
//     rates_historic.py

//     An example using the Darwinex ZeroMQ Connector for Python 3 and MetaTrader 4 PULL REQUEST
//     for v2.0.1 in which a Client requests rate history from EURGBP Daily from 2019.01.04 to
//     to 2019.01.14.

//     -------------------
//     Rates history:
//     -------------------
//     Through commmand HIST, this client can select multiple rates from an INSTRUMENT (symbol, timeframe).
//     For example, to receive rates from instruments EURUSD(M1), between two dates, it will send this
//     command to the Server, through its PUSH channel:

//     "HIST;EURUSD;1;2019.01.04 00:00:00;2019.01.14 00:00:00"

//     --

//     @author: [raulMrello](https://www.linkedin.com/in/raul-martin-19254530/)

// """
import (
	"fmt"
	"strings"
	"sync"
	"time"

	strategy_base "github.com/dinhero/zeromq-connector/examples/template/strategies/base/DWX_ZMQ_Strategy"
)

type RATES_HISTORIC struct {
	base     strategy_base.ZMQ_Strategy
	Delay    float32
	Verbose  bool
	Finished bool
	sync.Mutex
}

func (h *RATES_HISTORIC) Init(_name string, _delay float32, _broker_gmt int, _verbose bool) {
	h.base.Init(_name,
		map[string]interface{}{},
		_broker_gmt,
		map[string]interface{}{"OnPullData": h.OnPullData}, //# Registers itself as handler of pull data via self.onPullData()
		map[string]interface{}{"OnSubData": h.OnSubData},   //# Registers itself as handler of sub data via self.onSubData()
		_verbose)

	h.Delay = _delay
	h.Verbose = _verbose
	h.Finished = false
}

func (h *RATES_HISTORIC) IsFinished() bool {
	//""" Check if execution finished"""
	return h.Finished
}

// ##########################################################################
func (h *RATES_HISTORIC) OnPullData(data string) {
	// """
	//     Callback to process new data received through the PULL port
	//     """
	//     # print responses to request commands
	fmt.Println("Response from ExpertAdvisor=" + data)
}

func (h *RATES_HISTORIC) OnSubData(data string) {
	// """
	//     Callback to process new data received through the PULL port
	//     """
	//     # print responses to request commands
	_topic_msg := strings.Split(data, " ")
	fmt.Println("Data on Topic=" + _topic_msg[0] + " with Message=" + _topic_msg[1])
}

func (h *RATES_HISTORIC) Run() {
	// """
	//     Request historic data
	//     """

	h.Finished = false

	// Request Rates
	fmt.Println("Requesting Daily Rates from EURGBP")

	h.base.Zmq.DWX_MTX_SEND_HIST_REQUEST_("EURGBP", 1440, "2020.05.04 00:00:00", "2020.05.14 00:00:00")
	time.Sleep(time.Second * time.Duration(1))
	fmt.Println("History Data Dictionary:")
	fmt.Println(h.base.Zmq.History_DB)
	//# finishes (removes all subscriptions)
	h.Stop()

}

func (h *RATES_HISTORIC) Stop() {
	// """
	//   unsubscribe from all market symbols and exits
	//   """

	//   # remove subscriptions and stop symbols price feeding

	h.Lock()
	h.base.Zmq.DWX_MTX_UNSUBSCRIBE_ALL_MARKETDATA_REQUESTS_()
	fmt.Println("Unsubscribing from all topics")

	h.Unlock()
	time.Sleep(time.Second * time.Duration(h.Delay))

	h.Finished = true

}

// """ -----------------------------------------------------------------------------------------------
//     -----------------------------------------------------------------------------------------------
//     SCRIPT SETUP
//     -----------------------------------------------------------------------------------------------
//     -----------------------------------------------------------------------------------------------
// """
// if __name__ == "__main__":

//   # creates object with a predefined configuration: historic EURGBP_D1 between 4th adn 14th January 2019
//   print('Loading example...')
//   example = rates_historic()

//   # Starts example execution
//   print('unning example...')
//   example.run()

//   # Waits example termination
//   print('Waiting example termination...')
//   while not example.isFinished():
//     sleep(1)
//   print('Bye!!!')
