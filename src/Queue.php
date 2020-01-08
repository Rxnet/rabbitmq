<?php

/*
 * This file is part of the RxNET project.
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Rxnet\RabbitMq;

use Bunny\Channel;
use Bunny\Message as BaseMessage;
use Rx\Observable;
use Rx\ObserverInterface;

class Queue
{
    const PASSIVE = 'passive';
    const DURABLE = 'durable';
    const EXCLUSIVE = 'exclusive';
    const AUTO_DELETE = 'auto_delete';

    const DELETE_IF_EMPTY = 'if_empty';
    const DELETE_IF_UNUSED = 'if_unused';

    protected $queue_name;
    protected $channel;
    protected $exchange;

    public function __construct($queue_name, Channel $channel = null)
    {
        $this->channel = $channel;
        $this->queue_name = $queue_name;
    }

    public function setChannel(Channel $channel): Queue
    {
        $this->channel = $channel;

        return $this;
    }

    public function create($opts = [self::DURABLE]): Observable
    {
        if (!\is_array($opts)) {
            $opts = \func_get_args();
        }

        $params = [$this->queue_name];
        $params[] = \in_array(self::PASSIVE, $opts);
        $params[] = \in_array(self::DURABLE, $opts);
        $params[] = \in_array(self::EXCLUSIVE, $opts);
        $params[] = \in_array(self::AUTO_DELETE, $opts);

        $promise = \call_user_func_array([$this->channel, 'queueDeclare'], $params);

        return Observable::fromPromise($promise);
    }

    public function bind($routingKey, $exchange = 'amq.direct'): Observable
    {
        $promise = $this->channel->queueBind($this->queue_name, $exchange, $routingKey);

        return Observable::fromPromise($promise);
    }

    public function purge(): Observable
    {
        $promise = $this->channel->queuePurge($this->queue_name);

        return Observable::fromPromise($promise);
    }

    public function delete($opts = []): Observable
    {
        $params = [$this->queue_name];
        $params[] = \in_array(self::DELETE_IF_UNUSED, $opts);
        $params[] = \in_array(self::DELETE_IF_EMPTY, $opts);
        $promise = \call_user_func_array([$this->channel, 'queueDelete'], $params);

        return Observable::fromPromise($promise);
    }

    public function setQos($count = null, $size = null): Observable
    {
        $promise = $this->channel->qos($size, $count);

        return Observable::fromPromise($promise);
    }

    public function consume($consumerId = null, $opts = []): Observable
    {
        return Observable::create(
            function (ObserverInterface $observer) use ($consumerId, $opts) {
                $params = [
                    'callback' => [$observer, 'onNext'],
                    'queue' => $this->queue_name,
                    'consumerTag' => $consumerId,
                    'noLocal' => \in_array(Client::CHANNEL_NO_LOCAL, $opts, true),
                    'noAck' => \in_array(Client::CHANNEL_NO_ACK, $opts, true),
                    'exclusive' => \in_array(Client::CHANNEL_EXCLUSIVE, $opts, true),
                    'noWait' => \in_array(Client::CHANNEL_NO_WAIT, $opts, true),
                ];

                $promise = \call_user_func_array([$this->channel, 'consume'], $params);

                $promise->then(null, [$observer, 'onError']);
            })
            ->map(function (BaseMessage $message) {
                return new Message($this->channel, $message);
            });
    }

    /**
     * Pop one element from the queue.
     */
    public function get($queue, $noAck = false): Observable
    {
        $promise = $this->channel->get($queue, $noAck);

        return Observable::fromPromise($promise)
            ->map(function (BaseMessage $message) {
                return new Message($this->channel, $message);
            });
    }

    public function name(): string
    {
        return $this->queue_name;
    }
}
