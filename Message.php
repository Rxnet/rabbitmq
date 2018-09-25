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

class Message
{
    /** @var string */
    public $consumerTag;
    /** @var int */
    public $deliveryTag;
    /** @var bool */
    public $redelivered;
    /** @var string */
    public $exchange;
    /** @var string */
    public $routingKey;
    /** @var array */
    public $headers;

    protected $channel;
    protected $message;
    protected $trait;

    protected $data;
    protected $labels = [];

    const HEADER_TRIED = 'tried';
    const HEADER_LABELS = 'labels';

    public function __construct(Channel $channel, BaseMessage $message)
    {
        $this->channel = $channel;
        $this->message = $message;

        $this->consumerTag = $message->consumerTag;
        $this->deliveryTag = $message->deliveryTag;
        $this->redelivered = $message->redelivered;
        $this->exchange = $message->exchange;
        $this->routingKey = $message->routingKey;
        $this->headers = $message->headers;

        $this->headers[self::HEADER_TRIED] = isset($this->headers[self::HEADER_TRIED]) ? $this->headers[self::HEADER_TRIED] + 1 : 1;

        $this->data = $message->content;

        $this->labels = $this->headers[self::HEADER_LABELS] ?? null;
        try {
            $this->labels = json_decode($this->labels, true) ?? [];
        } catch (\InvalidArgumentException $e) {
            $this->labels = [];
        }
    }

    public function getRoutingKey(): string
    {
        return $this->routingKey;
    }

    public function getData()
    {
        return $this->data;
    }

    public function getHeader($header, $default = null)
    {
        return $this->headers[$header] ?? $default;
    }

    public function getLabels(): array
    {
        return $this->labels;
    }

    public function getLabel($label, $default = null)
    {
        return $this->labels[$label] ?? $default;
    }

    public function setLabel($label, $value): Message
    {
        $this->labels[$label] = $value;

        return $this;
    }

    public function getRawContent()
    {
        return $this->message->content;
    }

    public function ack(): Observable
    {
        $promise = $this->channel->ack($this->message);

        return Observable::fromPromise($promise);
    }

    /**
     * Not acknowledge and add at top of queue (will be next one).
     */
    public function nack($requeue = true): Observable
    {
        $promise = $this->channel->nack($this->message, false, $requeue);

        return Observable::fromPromise($promise);
    }

    public function reject($requeue = true): Observable
    {
        $promise = $this->channel->reject($this->message, $requeue);

        return Observable::fromPromise($promise);
    }

    protected function prepareHeaders($additionalHeaders = []): array
    {
        $headers = $this->headers;
        if (isset($headers[self::HEADER_LABELS])) {
            unset($headers[self::HEADER_LABELS]);
        }
        if ($this->labels) {
            $headers[self::HEADER_LABELS] = json_encode($this->labels);
        }

        return array_merge(
            $headers,
            $additionalHeaders
        );
    }

    public function retryLater($delay, $exchange = 'direct.delayed'): Observable
    {
        $headers = $this->prepareHeaders(['x-delay' => $delay]);

        return $this->reject(false)
            ->flatMap(function () use ($headers, $exchange) {
                return Observable::fromPromise(
                    $this->channel->publish(
                        $this->getData(),
                        $headers,
                        $exchange,
                        $this->routingKey
                    )
                );
            });
    }

    public function rejectToBottom(): Observable
    {
        return $this->reject(false)
            ->flatMap(function () {
                return Observable::fromPromise(
                    $this->channel->publish(
                        $this->getData(),
                        $this->prepareHeaders(),
                        $this->exchange,
                        $this->routingKey
                    )
                );
            });
    }
}
