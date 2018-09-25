<?php

/*
 * This file is part of the RxNET project.
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Rxnet\RabbitMq;

use Bunny\Async\Client as BaseAsynchClient;
use Bunny\Channel;
use React\EventLoop\LoopInterface;
use Rx\Observable;
use Rx\ObserverInterface;

class Client
{
    const MSG_REQUEUE = 'msg_requeue';
    const CHANNEL_EXCLUSIVE = 'channel_exclusive';
    const CHANNEL_NO_ACK = 'channel_no_ack';
    const CHANNEL_NO_LOCAL = 'channel_no_local';
    const CHANNEL_NO_WAIT = 'channel_no_wait';

    protected $loop;
    protected $bunny;
    protected $channel;
    protected $configuration;

    public function __construct(LoopInterface $loop, $configuration)
    {
        if (\is_string($configuration)) {
            $configuration = parse_url($configuration);
            $configuration['vhost'] = $configuration['path'];
        }
        $this->configuration = $configuration;
        $this->loop = $loop;
    }

    public function connect(): Observable
    {
        $this->bunny = new BaseAsynchClient($this->loop, $this->configuration);

        return Observable::fromPromise(
            $this->bunny
                ->connect()
                ->then(function (BaseAsynchClient $client) {
                    return $client->channel();
                })
                ->then(function (Channel $channel) {
                    // set a default channel
                    $this->channel = $channel;

                    return $channel;
                })
        );
    }

    /**
     * Open a new channel and attribute it to given queues or exchanges.
     *
     * @param Queue[]|Exchange[] $bind
     */
    public function channel($bind = []): Observable
    {
        $this->bunny = $this->bunny ?? new BaseAsynchClient($this->loop, $this->configuration);

        if (!\is_array($bind)) {
            $bind = \func_get_args();
        }

        return Observable::fromPromise(
            $this->bunny
                ->connect()
                ->then(function (BaseAsynchClient $client) {
                    return $client->channel();
                })
                ->then(function (Channel $channel) use ($bind) {
                    foreach ($bind as $obj) {
                        $obj->setChannel($channel);
                    }

                    return $channel;
                })
        );
    }

    /**
     * Consume given queue at.
     */
    public function consume($queue, $prefetchCount = null, $prefetchSize = null, $consumerId = null): Observable
    {
        return $this->channel()
            ->do(function (Channel $channel) use ($prefetchCount, $prefetchSize) {
                $channel->qos($prefetchSize, $prefetchCount);
            })
            ->flatMap(
                function (Channel $channel) use ($queue, $consumerId) {
                    return $this->queue($queue, $channel)
                        ->consume($consumerId);
                }
            );
    }

    /**
     * One time produce on dedicated channel and close after.
     */
    public function produce($data, $routingKey, $exchange = '', $headers = []): Observable
    {
        return Observable::create(function (ObserverInterface $observer) use ($exchange, $data, $headers, $routingKey) {
            return $this->channel()
                ->flatMap(
                    function (Channel $channel) use ($exchange, $data, $headers, $routingKey) {
                        return $this->exchange($exchange, $channel)
                            ->produce($data, $routingKey, $headers)
                            ->do(function () use ($channel) {
                                $channel->close();
                            });
                    }
                )->subscribe($observer);
        });
    }

    public function queue($name, Channel $channel = null): Queue
    {
        $channel = ($channel) ?: $this->channel;

        return new Queue($name, $channel);
    }

    public function exchange($name = 'amq.direct', Channel $channel = null): Exchange
    {
        $channel = ($channel) ?: $this->channel;

        return new Exchange($name, $channel);
    }
}
