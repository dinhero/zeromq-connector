package rates_subscriptions

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	strategy_base "github.com/dinhero/zeromq-connector/examples/template/strategies/base/DWX_ZMQ_Strategy"
)

type RATES_SUBSCRIPTIONS struct {
	base        strategy_base.ZMQ_Strategy
	Instruments map[string]interface{}
	Delay       float32
	Verbose     bool
	Finished    bool
	Eurusd_cnt  int
	Gdaxi_cnt   int
	sync.Mutex
}

func (h *RATES_SUBSCRIPTIONS) Init(_name string, _instruments map[string]interface{}, _delay float32, _broker_gmt int, _verbose bool) {
	h.base.Init(_name,
		map[string]interface{}{},
		_broker_gmt,
		map[string]interface{}{"OnPullData": h}, //# Registers itself as handler of pull data via self.onPullData()
		map[string]interface{}{"OnSubData": h},  //# Registers itself as handler of sub data via self.onSubData()
		_verbose)

	h.Instruments = _instruments
	h.Delay = _delay
	h.Verbose = _verbose
	h.Finished = false

	h.Eurusd_cnt = 0
	h.Gdaxi_cnt = 0
}

func (h *RATES_SUBSCRIPTIONS) IsFinished() bool {
	//""" Check if execution finished"""
	return h.Finished
}

// ##########################################################################
func (h *RATES_SUBSCRIPTIONS) OnPullData(data string) {
	// """
	//     Callback to process new data received through the PULL port
	//     """
	//     # print responses to request commands
	fmt.Println("Response from ExpertAdvisor=" + data)
}
func (h *RATES_SUBSCRIPTIONS) OnSubData(data string) {
	// """
	//     Callback to process new data received through the SUB port
	//     """
	//     # split msg to get topic and message

	_topic_msg := strings.Split(data, " ")
	fmt.Println("Data on Topic = " + _topic_msg[0] + " with Message=" + _topic_msg[1])

	if _topic_msg[0] == "EURUSD_M1" {
		h.Eurusd_cnt++
	}
	if _topic_msg[1] == "GDAXI_M5" {
		h.Gdaxi_cnt++
	}

	//# check if received at least 5 prices from EURUSD to cancel its feed

	if h.Eurusd_cnt >= 5 {
		// # updates the instrument list and request the update to the Expert Advisor

		h.Instruments["GDAXI_M5"] = []interface{}{"GDAXI_M5", "GDAXI", 5}
		h.Subscribe_to_rate_feeds()
		//# resets counters
		h.Eurusd_cnt = 0

	}

	if h.Gdaxi_cnt >= 3 {
		h.Stop()
		fmt.Println(h.base.Zmq.Market_Data_DB)
	}
}

func (h *RATES_SUBSCRIPTIONS) Run() {
	// """
	//     Starts price subscriptions
	//     """
	h.Finished = false
	//        # Subscribe to all symbols in self._symbols to receive bid,ask prices
	h.Subscribe_to_rate_feeds()
}

func (h *RATES_SUBSCRIPTIONS) Stop() {
	// """
	//   unsubscribe from all market symbols and exits
	//   """

	//   # remove subscriptions and stop symbols price feeding

	h.Lock()
	h.base.Zmq.DWX_MTX_UNSUBSCRIBE_ALL_MARKETDATA_REQUESTS_()
	fmt.Println("Unsubscribing from all topics")

	h.Unlock()
	time.Sleep(time.Second * time.Duration(h.Delay))

	h.Lock()
	h.base.Zmq.DWX_MTX_SEND_TRACKPRICES_REQUEST_(map[string]interface{}{})
	fmt.Println("Removing symbols list")
	time.Sleep(time.Second * time.Duration(h.Delay))
	h.base.Zmq.DWX_MTX_SEND_TRACKRATES_REQUEST_([][]interface{}{})
	fmt.Println("Removing instruments list")
	h.Unlock()
	time.Sleep(time.Second * time.Duration(h.Delay))

	h.Finished = true

}

func (h *RATES_SUBSCRIPTIONS) Subscribe_to_rate_feeds() {
	// """
	//   Starts the subscription to the self._symbols list setup during construction.
	//   1) Setup symbols in Expert Advisor through self._zmq._DWX_MTX_SUBSCRIBE_MARKETDATA_
	//   2) Starts price feeding through self._zmq._DWX_MTX_SEND_TRACKPRICES_REQUEST_
	//   """

	if len(h.Instruments) > 0 {
		//# subscribe to all symbols price feeds
		for _, _instrument := range h.Instruments {
			h.Lock()
			data := _instrument.([]interface{})[0].(string)

			h.base.Zmq.DWX_MTX_SUBSCRIBE_MARKETDATA_(string(data), ";")

			fmt.Println("Subscribed to " + _instrument.(string) + " price feed")

			h.Unlock()
			time.Sleep(time.Second * time.Duration(h.Delay))

		}
		//# configure symbols to receive price feeds
		h.Lock()
		h.base.Zmq.DWX_MTX_SEND_TRACKPRICES_REQUEST_(h.Instruments)
		string_instrument, _ := json.Marshal(h.Instruments)
		fmt.Println("Configuring rate feed for " + string(string_instrument) + " instruments")

		h.Unlock()
		time.Sleep(time.Second * time.Duration(h.Delay))
	}

}

// """ -----------------------------------------------------------------------------------------------
//     -----------------------------------------------------------------------------------------------
//     SCRIPT SETUP
//     -----------------------------------------------------------------------------------------------
//     -----------------------------------------------------------------------------------------------
// """
// if __name__ == "__main__":

//   # creates object with a predefined configuration: intrument list including EURUSD_M1 and GDAXI_M5
//   print('Loading example...')
//   example = rates_subscriptions()

//   # Starts example execution
//   print('unning example...')
//   example.run()

//   # Waits example termination
//   print('Waiting example termination...')
//   while not example.isFinished():
//     sleep(1)
//   print('Bye!!!')
