package prices_subscriptions

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	strategy_base "github.com/dinhero/zeromq-connector/examples/template/strategies/base/DWX_ZMQ_Strategy"
)

type Prices_subscriptions struct {
	base       strategy_base.ZMQ_Strategy
	Symbols    map[string]interface{}
	Delay      float32
	Verbose    bool
	Finished   bool
	Eurusd_cnt int
	Gdaxi_cnt  int
	handlers   func()
	sync.Mutex
}

func (p *Prices_subscriptions) Init(_name string, _symbols map[string]interface{}, _delay float32, _broker_gmt int, _verbose bool) {
	p.base.Init(_name,
		_symbols,
		_broker_gmt,
		map[string]interface{}{"OnPullData": p}, //# Registers itself as handler of pull data via self.onPullData()
		map[string]interface{}{"OnSubData": p},  //# Registers itself as handler of sub data via self.onSubData()
		_verbose)

	//# This strategy's variables
	p.Symbols = _symbols
	p.Delay = _delay
	p.Verbose = _verbose
	p.Finished = false

	//# Initializes counters of number of prices received from each symbol
	p.Eurusd_cnt = 0
	p.Gdaxi_cnt = 0

	//# lock for acquire/release of ZeroMQ connector
	// p.Lock = Lock()

}

// ##########################################################################
func (p *Prices_subscriptions) IsFinished() bool {
	//""" Check if execution finished"""
	return p.Finished
}

// ##########################################################################
func (p *Prices_subscriptions) OnPullData(data string) {
	// """
	//     Callback to process new data received through the PULL port
	//     """
	//     # print responses to request commands
	fmt.Println("Response from ExpertAdvisor=" + data)
}

func (p *Prices_subscriptions) OnSubData(data string) {
	// """
	//     Callback to process new data received through the SUB port
	//     """
	//     # split msg to get topic and message

	_topic_msg := strings.Split(data, " ")
	fmt.Println("Data on Topic = " + _topic_msg[0] + " with Message=" + _topic_msg[1])

	if _topic_msg[0] == "EURUSD" {
		p.Eurusd_cnt++
	}
	if _topic_msg[1] == "GDAXI" {
		p.Gdaxi_cnt++
	}

	if p.Eurusd_cnt >= 10 && p.Gdaxi_cnt >= 10 {
		var symbol map[string]interface{}
		symbol["EURUSD"] = nil
		p.Symbols = symbol
		p.Subscribe_to_price_feeds()
		p.Eurusd_cnt = 0
		p.Gdaxi_cnt = 0

	}

	if p.Eurusd_cnt >= 10 && len(p.Symbols) == 1 {
		p.Stop()
	}
}

func (p *Prices_subscriptions) Run() {
	// """
	//     Starts price subscriptions
	//     """
	p.Finished = false
	//        # Subscribe to all symbols in self._symbols to receive bid,ask prices
	p.Subscribe_to_price_feeds()
}

func (p *Prices_subscriptions) Stop() {
	// """
	//   unsubscribe from all market symbols and exits
	//   """

	//   # remove subscriptions and stop symbols price feeding

	p.Lock()
	p.base.Zmq.DWX_MTX_UNSUBSCRIBE_ALL_MARKETDATA_REQUESTS_()
	fmt.Println("Unsubscribing from all topics")

	p.Unlock()
	time.Sleep(time.Second * time.Duration(p.Delay))

	p.Lock()
	p.base.Zmq.DWX_MTX_SEND_TRACKPRICES_REQUEST_(map[string]interface{}{})
	fmt.Println("Removing symbols list")
	time.Sleep(time.Second * time.Duration(p.Delay))
	p.base.Zmq.DWX_MTX_SEND_TRACKRATES_REQUEST_([][]interface{}{})
	fmt.Println("Removing instruments list")
	p.Unlock()
	time.Sleep(time.Second * time.Duration(p.Delay))

	p.Finished = true

}
func (p *Prices_subscriptions) Subscribe_to_price_feeds() {
	// """
	//   Starts the subscription to the self._symbols list setup during construction.
	//   1) Setup symbols in Expert Advisor through self._zmq._DWX_MTX_SUBSCRIBE_MARKETDATA_
	//   2) Starts price feeding through self._zmq._DWX_MTX_SEND_TRACKPRICES_REQUEST_
	//   """

	if len(p.Symbols) > 0 {
		//# subscribe to all symbols price feeds
		for _symbol := range p.Symbols {
			p.Lock()
			p.base.Zmq.DWX_MTX_SUBSCRIBE_MARKETDATA_(_symbol, ";")
			fmt.Println("Subscribed to " + _symbol + " price feed")

			p.Unlock()
			time.Sleep(time.Second * time.Duration(p.Delay))

		}
		//# configure symbols to receive price feeds
		p.Lock()
		p.base.Zmq.DWX_MTX_SEND_TRACKPRICES_REQUEST_(p.Symbols)
		fmt.Println("Configuring price feed for " + strconv.Itoa(len(p.Symbols)) + " symbols")
		p.Unlock()
		time.Sleep(time.Second * time.Duration(p.Delay))
	}

}

// """ -----------------------------------------------------------------------------------------------
//     -----------------------------------------------------------------------------------------------
//     SCRIPT SETUP
//     -----------------------------------------------------------------------------------------------
//     -----------------------------------------------------------------------------------------------
// """
// if __name__ == "__main__":

//   # creates object with a predefined configuration: symbol list including EURUSD and GDAXI
//   print('Loading example...')
//   example = prices_subscriptions()

//   # Starts example execution
//   print('unning example...')
//   example.run()

//   # Waits example termination
//   print('Waiting example termination...')
//   while not example.isFinished():
//     sleep(1)
//   print('Bye!!!')
