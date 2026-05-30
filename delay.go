package warren

// DelayedTopic returns the Exchange literal for a topic-routed delayed-message
// exchange: Kind=ExchangeDelayed with the x-delayed-type=topic argument the
// rabbitmq_delayed_message_exchange plugin requires. Declare it via a Topology and
// publish to it with Message[M].Delay set to schedule delivery.
//
// Plugin requirement. ExchangeDelayed needs the rabbitmq_delayed_message_exchange
// plugin enabled on every broker node; declaring one against a broker without the
// plugin fails with a command-invalid channel error.
//
// Durability caveat (load-bearing). The exchange is declared Durable so its
// definition survives a broker restart, but the plugin stores SCHEDULED messages
// in a node-local, non-replicated table. A publisher confirm means "accepted for
// scheduling", not "will be delivered": if the owning node fails before the delay
// elapses, the scheduled message is lost silently — even with durable topology and
// confirms on. This is the one path where a confirmed publish can still be lost.
// For delays that must survive node failure, prefer a durable (ideally quorum)
// queue with x-message-ttl plus a DLX (see Message.Delay).
func DelayedTopic(name string) Exchange {
	return Exchange{
		Name:    name,
		Kind:    ExchangeDelayed,
		Durable: true,
		Args:    map[string]any{"x-delayed-type": string(ExchangeTopic)},
	}
}
