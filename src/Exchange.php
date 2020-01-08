<?php

/*
 * This file is part of the RxNET project.
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Rxnet\RabbitMq;

use Bunny\Channel;
use Rx\Observable;

class Exchange
{
    const DELIVERY_RAM = 2;
    const DELIVERY_DISK = 1;
    const TYPE_DIRECT = 'direct';
    const PASSIVE = 'passive';
    const DURABLE = 'durable';
    const AUTO_DELETE = 'auto_delete';

    protected $channel;
    protected $exchange;

    public function __construct($exchange = 'amq.direct', Channel $channel = null)
    {
        $this->channel = $channel;
        $this->exchange = $exchange;
    }

    public function create($type = self::TYPE_DIRECT, $opts = []): Observable
    {
        $params = [$this->exchange, $type];

        $params[] = \in_array(self::PASSIVE, $opts);
        $params[] = \in_array(self::DURABLE, $opts);
        $params[] = \in_array(self::AUTO_DELETE, $opts);
        $promise = \call_user_func_array([$this->channel, 'exchangeDeclare'], $params);

        return Observable::fromPromise($promise);
    }

    public function setChannel(Channel $channel): Exchange
    {
        $this->channel = $channel;

        return $this;
    }

    public function produce($data, $routingKey, $headers = [], $delivery = self::DELIVERY_DISK): Observable
    {
        if (self::DELIVERY_DISK === $delivery) {
            $headers['delivery-mode'] = 2;
        }

        $promise = $this->channel->publish($data, $headers, $this->exchange, $routingKey);

        return Observable::fromPromise($promise);
    }
}
