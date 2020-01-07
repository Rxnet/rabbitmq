<?php

/*
 * This file is part of the RxNET project.
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Rxnet\RabbitMq;

use Bunny\Channel;
use Bunny\Message as BaseMessage;
use React\Promise\PromiseInterface;
use Rx\Observable;

class Message
{
    /** @var string */
    protected $consumer_tag;
    /** @var int */
    protected $delivery_tag;
    /** @var bool */
    protected $redelivered;
    /** @var string */
    protected $exchange;
    /** @var string */
    protected $routing_key;
    /** @var array */
    protected $headers = [];
    /** @var string */
    protected $content;
    /** @var Channel */
    protected $channel;
    /** @var BaseMessage  */
    protected $base_message;

    const HEADER_TRIED = 'tried';
    const HEADER_DELAY = 'x-delay';

    public function __construct(Channel $channel, BaseMessage $base_message)
    {
        $this->channel = $channel;
        $this->base_message = $base_message;

        $this->consumer_tag = $base_message->consumerTag;
        $this->delivery_tag = $base_message->deliveryTag;
        $this->redelivered = $base_message->redelivered;
        $this->exchange = $base_message->exchange;
        $this->routing_key = $base_message->routingKey;
        $this->headers = $base_message->headers;

        $this->headers[self::HEADER_TRIED] = isset($this->headers[self::HEADER_TRIED]) ? $this->headers[self::HEADER_TRIED] + 1 : 1;

        $this->content = $base_message->content;
    }

    public function channel(): Channel
    {
        return $this->channel;
    }

    public function message(): BaseMessage
    {
        return $this->base_message;
    }

    public function consumerTag(): string
    {
        return $this->consumer_tag;
    }

    public function deliveryTag(): int
    {
        return $this->delivery_tag;
    }

    public function redelivered(): bool
    {
        return $this->redelivered;
    }

    public function exchange(): string
    {
        return $this->exchange;
    }

    public function routingKey(): string
    {
        return $this->routing_key;
    }

    public function content(): string
    {
        return $this->content;
    }

    public function headers(): array
    {
        return $this->headers;
    }

    public function header(string $header, $default = null)
    {
        return $this->headers[$header] ?? $default;
    }

    public function addHeader(string $header, $value): self
    {
        if (!is_scalar($value)) {
            throw new \InvalidArgumentException(sprintf(
                'Header must be a scalar value, %s given',
                gettype($value)
            ));
        }

        $this->headers[$header] = $value;

        return $this;
    }

    public function ack(): Observable
    {
        $promise = $this->channel->ack($this->base_message);

        if (!$promise instanceof PromiseInterface) {
            throw new \RuntimeException('Promise can\'t be returned');
        }

        return Observable::fromPromise($promise);
    }

    /**
     * Not acknowledge and add at top of queue (will be next one).
     */
    public function nack($requeue = true): Observable
    {
        $promise = $this->channel->nack($this->base_message, false, $requeue);

        if (!$promise instanceof PromiseInterface) {
            throw new \RuntimeException('Promise can\'t be returned');
        }

        return Observable::fromPromise($promise);
    }

    public function reject($requeue = true): Observable
    {
        $promise = $this->channel->reject($this->base_message, $requeue);

        if (!$promise instanceof PromiseInterface) {
            throw new \RuntimeException('Promise can\'t be returned');
        }

        return Observable::fromPromise($promise);
    }

    public function retryLater($delay, $exchange = 'direct.delayed'): Observable
    {
        $headers = array_merge(
            $this->headers(),
            [self::HEADER_DELAY => $delay]
        );

        return $this->reject(false)
            ->flatMap(function () use ($headers, $exchange) {
                $promise = $this->channel->publish(
                    $this->content(),
                    $headers,
                    $exchange,
                    $this->routing_key
                );

                if (!$promise instanceof PromiseInterface) {
                    throw new \RuntimeException('Promise can\'t be returned');
                }

                return Observable::fromPromise($promise);
            });
    }

    public function rejectToBottom(): Observable
    {
        return $this->reject(false)
            ->flatMap(function () {
                $promise = $this->channel->publish(
                    $this->content(),
                    $this->headers(),
                    $this->exchange,
                    $this->routing_key
                );

                if (!$promise instanceof PromiseInterface) {
                    throw new \RuntimeException('Promise can\'t be returned');
                }

                return Observable::fromPromise($promise);
            });
    }
}
