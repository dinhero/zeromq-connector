package coin_flip_traders

import (
	"fmt"
	"sync"
	"time"
	strategy_base "zeromq-connector/examples/template/strategies/base/DWX_ZMQ_Strategy"
)

type COIN_FLIP_TRADERS struct {
	base          strategy_base.ZMQ_Strategy
	Traders       map[string]interface{}
	Market_open   bool
	Max_trades    int
	Close_t_delta int
	Delay         float32
	Verbose       bool
	sync.Mutex
}

func (c *COIN_FLIP_TRADERS) Init(_name string,
	_symbols map[string]interface{},
	_delay float32,
	_broker_gmt int,
	_verbose bool,
	_max_trades int,
	_close_t_delta int) {

	// def __init__(self, _name="COIN_FLIP_TRADERS",
	//          _symbols=[('EURUSD',0.01),
	//                    ('AUDNZD',0.01),
	//                    ('NDX',0.10),
	//                    ('UK100',0.1),
	//                    ('GDAXI',0.01),
	//                    ('XTIUSD',0.01),
	//                    ('SPX500',1.0),
	//                    ('STOXX50E',0.10),
	//                    ('XAUUSD',0.01)],
	//          _delay=0.1,
	//          _broker_gmt=3,
	//          _verbose=False,

	//          _max_trades=1,
	//          _close_t_delta=5):
	c.base.Init(_name, _symbols, _broker_gmt, map[string]interface{}{}, map[string]interface{}{}, _verbose)

	//# This strategy's variables
	c.Traders = map[string]interface{}{}
	c.Market_open = true
	c.Max_trades = _max_trades
	c.Close_t_delta = _close_t_delta
	c.Delay = _delay
	c.Verbose = _verbose
	// c.Lock = Lock()
}

func (c *COIN_FLIP_TRADERS) Run_() {
	// """
	// Logic:

	// 	For each symbol in self._symbols:

	// 		1) Open a new Market Order every 2 seconds
	// 		2) Close any orders that have been running for 10 seconds
	// 		3) Calculate Open P&L every second
	// 		4) Plot Open P&L in real-time
	// 		5) Lot size per trade = 0.01
	// 		6) SL/TP = 10 pips each
	// """

	// # Launch traders!
	for _symbol, _symbolValue := range c.base.Symbols {

		go c.Trader_(_symbol, _symbolValue, c.Max_trades)

		fmt.Println("[" + _symbol + "_Trader] Alright, here we go.. Gerrrronimooooooooooo!  ..... xD")
	}

	fmt.Println("+--------------+\n+ LIVE UPDATES +\n+--------------+")
	// # _verbose can print too much information.. so let's start a thread
	// # that prints an update for instructions flowing through ZeroMQ

	go c.Updater_(c.Delay)

}

// ##########################################################################
func (c *COIN_FLIP_TRADERS) Updater_(_delay float32) {
	for c.Market_open {
		//# Acquire lock
		c.Lock()
		fmt.Println(c.base.Zmq.Get_response_())
		//# Release lock
		c.Unlock()
		time.Sleep(time.Second * time.Duration(_delay))

	}
}

func (c *COIN_FLIP_TRADERS) Trader_(_symbol string, _symbolValue interface{}, _max_trades int) {
	defer func() {
		c.Unlock()
		time.Sleep(time.Second * time.Duration(c.Delay))
	}()

	_default_order := c.base.Zmq.Generate_default_order_dict()
	_default_order["_symbol"] = _symbol
	_default_order["_lots"] = _symbolValue
	_default_order["_SL"] = 100
	_default_order["_TP"] = 100
	_default_order["_comment"] = _symbol + "_Trader"

	// """
	//     Default Order:
	//     --
	//     {'_action': 'OPEN',
	//      '_type': 0,
	//      '_symbol': EURUSD,
	//      '_price':0.0,
	//      '_SL': 100,                     # 10 pips
	//      '_TP': 100,                     # 10 pips
	//      '_comment': 'EURUSD_Trader',
	//      '_lots': 0.01,
	//      '_magic': 123456}
	//     """

	for c.Market_open {
		// # Acquire lock
		c.Lock()
		// #############################
		// # SECTION - GET OPEN TRADES #
		// #############################

		_ot := c.base.Reporting.Get_open_trades_(_symbol+"_Trader", float64(c.Delay), 10)

		// # Reset cycle if nothing received
		if !c.base.Zmq.Valid_response_(_ot.String()) {
			continue
		}

	}

}

// ##########################################################################
func (c *COIN_FLIP_TRADERS) Stop_() {

	c.Market_open = false

	//Join all threads to main thread from doing anything
	for index, _ := range c.Traders {

		//# Setting _market_open to False will stop each "trader" thread
		//# from doing anything more. So wait for them to finish.
		// _t.join()

		fmt.Println("[" + index + "] .. and that's a wrap! Time to head home.")
	}
	//       # Kill the updater too
	// c.Updater_.join()

	// fmt.Println("{} .. wait for me.... I\'m going home too! xD".format(self._updater_.getName()))

	//# Send mass close instruction to MetaTrader in case anything's left.
	c.base.Zmq.DWX_MTX_CLOSE_ALL_TRADES_()
}

// ##########################################################################

// """ -----------------------------------------------------------------------------------------------
//     -----------------------------------------------------------------------------------------------
//     SCRIPT SETUP
//     -----------------------------------------------------------------------------------------------
//     -----------------------------------------------------------------------------------------------
// """
// # IMPORTANT: don't execute this on a live account!
// # if __name__ == "__main__":

// #     # creates object with a predefined configuration: symbol list including EURUSD and GDAXI
// #     print('Loading example...')
// #     example = coin_flip_traders()

// #     # Starts example execution.
// #     print('unning example...')
// #     example._run_()

// #     # Waits example termination
// #     print('Waiting example termination...')
// #     sleep(10)
// #     print('Stopping and closing all positions again...')
// #     example._stop_()
