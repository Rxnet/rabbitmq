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
use Rx\Subject\ReplaySubject;

class Client
{
    const MSG_REQUEUE = 'msg_requeue';
    const CHANNEL_EXCLUSIVE = 'channel_exclusive';
    const CHANNEL_NO_ACK = 'channel_no_ack';
    const CHANNEL_NO_LOCAL = 'channel_no_local';
    const CHANNEL_NO_WAIT = 'channel_no_wait';

    protected $loop;
    /** @var BaseAsynchClient|null */
    protected $bunny;
    /** @var Observable|null */
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

    public function init(): void
    {
        $this->bunny = new BaseAsynchClient($this->loop, $this->configuration);

        if (null !== $this->channel) {
            $this->close();
        }
    }

    public function channel(): Observable
    {
        if (null === $this->bunny) {
            $this->init();
        }

        if (null !== $this->channel) {
            return $this->channel;
        }

        $this->channel = $replay = new ReplaySubject(1);

        $connectionPromise = Observable::fromPromise(
            $this->bunny
                ->connect()
                ->then(function (BaseAsynchClient $client) {
                    return $client->channel();
                })
        );

        $connectionPromise->takeLast(1)->subscribe($replay);

        return $this->channel;
    }

    /**
     * Open a new channel and attribute it to given queues or exchanges.
     *
     * @param array<Queue|Exchange> $bind
     */
    public function bind($bind = []): Observable
    {
        if (!\is_array($bind)) {
            $bind = \func_get_args();
        }

        return $this->channel()
            ->map(function (Channel $channel) use ($bind) {
                foreach ($bind as $obj) {
                    $obj->setChannel($channel);
                }

                return $channel;
            });
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
                    return (new Queue($queue, $channel))
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
                        return (new Exchange($exchange, $channel))
                            ->produce($data, $routingKey, $headers);
                    }
                )->subscribe($observer);
        });
    }

    public function close(): Observable
    {
        return $this->channel()
            ->flatMap(function (Channel $channel) {
                Observable::fromPromise($channel->close())
                    ->do(function () {
                        $this->channel = null;
                    });
            });
    }
}
