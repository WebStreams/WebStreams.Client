// <summary>
//   WebStream methods.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Dapr.WebStreams.Client
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Reactive.Concurrency;
    using System.Reactive.Disposables;
    using System.Reactive.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Dapr.WebSockets;

    using Newtonsoft.Json;

    /// <summary>
    /// WebStream methods.
    /// </summary>
    public static class WebStream
    {
        /// <summary>
        /// Connects to the WebSocket specified by <see cref="uri"/>, piping in all provided observables, and
        /// returning an observable collection of objects from the endpoint.
        /// </summary>
        /// <param name="uri">
        /// The URI.
        /// </param>
        /// <param name="inputStreams">
        /// The name-value collection of input streams to pipe to the endpoint.
        /// </param>
        /// <param name="serializerSettings">
        /// The serializer Settings.
        /// </param>
        /// <param name="scheduler">The scheduler.</param>
        /// <typeparam name="T">
        /// The underlying type returned by the endpoint.
        /// </typeparam>
        /// <returns>
        /// An observable collection of objects from the endpoint.
        /// </returns>
        public static IObservable<T> Create<T>(
            Uri uri,
            IDictionary<string, IObservable<object>> inputStreams = null,
            JsonSerializerSettings serializerSettings = null,
            IScheduler scheduler = null)
        {
            serializerSettings = serializerSettings ?? JsonConvert.DefaultSettings();
            scheduler = scheduler ?? new TaskPoolScheduler(Task.Factory);
            return Observable.Create<T>(
                incoming =>
                {
                    var cancellation = new CancellationTokenSource();
                    var exception = default(Exception);
                    try
                    {
                        // Connect to the WebSocket and pipe all input streams into it.
                        Action<string> onNext = next =>
                        {
                            if (cancellation.IsCancellationRequested)
                            {
                                throw new OperationCanceledException("Cancellation requested.");
                            }

                            try
                            {
                                if (string.IsNullOrWhiteSpace(next))
                                {
                                    return;
                                }

                                switch (next[0])
                                {
                                    case ResponseKind.Next:
                                        incoming.OnNext(JsonConvert.DeserializeObject<T>(next.Substring(1), serializerSettings));
                                        break;
                                    case ResponseKind.Error:
                                        var error = next.Substring(1);
                                        exception = new WebStreamException(error);
                                        cancellation.Cancel();
                                        break;
                                    case ResponseKind.Completed:
                                        cancellation.Cancel();
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                exception = new WebStreamException("OnNext failed.", e);
                                cancellation.Cancel();
                            }
                        };
                        Action<Exception> onError = e =>
                        {
                            if (cancellation.IsCancellationRequested)
                            {
                                throw new OperationCanceledException("Cancellation requested.");
                            }

                            exception = e;
                            cancellation.Cancel();
                        };
                        Action onClosed = () =>
                        {
                            if (cancellation.IsCancellationRequested)
                            {
                                throw new OperationCanceledException("Cancellation requested.");
                            }

                            cancellation.Cancel();
                        };

                        var socket = WebSocket.Connect(uri, cancellation.Token);
                        var incomingSubscription = socket.Subscribe(onNext, onError, onClosed);

                        cancellation.Token.Register(
                            () =>
                            {
                                // If we didn't cancel with an error, complete the incoming messages stream.
                                if (exception == null)
                                {
                                    if (cancellation.IsCancellationRequested)
                                    {
                                        incoming.OnError(new OperationCanceledException("Cancellation requested."));
                                    }
                                    else
                                    {
                                        incoming.OnCompleted();
                                    }
                                }
                                else
                                {
                                    incoming.OnError(exception);
                                }

                                incomingSubscription.Dispose();
                            });

                        // Connect to the socket and subscribe to the 'outgoing' observable while the connection remains opened.
                        SerializeOutgoingMessages(inputStreams, socket, serializerSettings, cancellation.Token, scheduler);
                    }
                    catch (Exception e)
                    {
                        exception = e;
                        cancellation.Cancel();
                        throw;
                    }

                    // Return a disposable which will unwind everything.
                    return Disposable.Create(cancellation.Cancel);
                }).ObserveOn(scheduler).SubscribeOn(scheduler);
        }

        /// <summary>
        /// Serializes outgoing messages.
        /// </summary>
        /// <param name="inputStreams">
        /// The input streams.
        /// </param>
        /// <param name="outgoing">
        /// The outgoing stream.
        /// </param>
        /// <param name="serializerSettings">
        /// The serializer settings.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <param name="scheduler">
        /// The scheduler.
        /// </param>
        /// <exception cref="OperationCanceledException">
        /// Cancellation was requested.
        /// </exception>
        private static void SerializeOutgoingMessages(
            IEnumerable<KeyValuePair<string, IObservable<object>>> inputStreams,
            IObserver<string> outgoing,
            JsonSerializerSettings serializerSettings,
            CancellationToken cancellationToken,
            IScheduler scheduler)
        {
            if (inputStreams == null)
            {
                return;
            }

            foreach (var input in inputStreams)
            {
                var name = input.Key;
                var values = input.Value.ObserveOn(scheduler).SubscribeOn(scheduler);

                var subscription = values.Subscribe(
                    next =>
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            throw new OperationCanceledException("Cancellation requested.");
                        }

                        try
                        {
                            var msg = ResponseKind.Next + name + '.' + JsonConvert.SerializeObject(next, serializerSettings);
                            outgoing.OnNext(msg);
                        }
                        catch (Exception e)
                        {
                            outgoing.OnNext(ResponseKind.Error + name + '.' + e.Message);
                        }
                    },
                    exception =>
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            throw new OperationCanceledException("Cancellation requested.");
                        }

                        outgoing.OnNext(ResponseKind.Error + name + '.' + exception.Message);
                    },
                    () =>
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            throw new OperationCanceledException("Cancellation requested.");
                        }

                        outgoing.OnNext(ResponseKind.Completed + name);
                    });
                cancellationToken.Register(
                    () =>
                    {
                        Debug.WriteLine("inputstream " + name + " cancellation called");
                        subscription.Dispose();
                    });
            }
        }
    }
}