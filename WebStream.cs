// <summary>
//   WebStream methods.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Dapr.WebStreams.Client
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reactive.Disposables;
    using System.Reactive.Linq;
    using System.Reactive.Threading.Tasks;
    using System.Threading;

    using Dapr.WebSockets;

    using Newtonsoft.Json;

    /// <summary>
    /// WebStream methods.
    /// </summary>
    public static class WebStream
    {
        /// <summary>
        /// Gets or sets the default serializer settings.
        /// </summary>
        public static JsonSerializerSettings DefaultSerializerSettings { get; set; }

        /// <summary>
        /// Connects to the WebSocket specified by <see cref="uri"/> and returns an observable collection of objects
        /// from the endpoint.
        /// </summary>
        /// <param name="uri">
        /// The URI.
        /// </param>
        /// <param name="serializerSettings">
        /// The serializer settings.
        /// </param>
        /// <typeparam name="T">
        /// The underlying type returned by the endpoint.
        /// </typeparam>
        /// <returns>
        /// An observable collection of objects from the endpoint.
        /// </returns>
        public static IObservable<T> Create<T>(Uri uri, JsonSerializerSettings serializerSettings = null)
        {
            serializerSettings = serializerSettings ?? DefaultSerializerSettings;
            return Observable.Create<T>(
                observer =>
                {
                    var cancellation = new CancellationTokenSource();
                    var subscription = new IDisposable[] { null };
                    Action dispose = () =>
                    {
                        if (subscription[0] != null)
                        {
                            subscription[0].Dispose();
                        }

                        cancellation.Dispose();
                    };

                    var socket = WebSocket.ConnectOutput(uri, cancellation.Token).ToObservable().Merge();
                    subscription[0] = SubscribeToSocket(socket, observer, dispose, serializerSettings);
                    return Disposable.Create(dispose);
                });
        }

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
        /// <typeparam name="T">
        /// The underlying type returned by the endpoint.
        /// </typeparam>
        /// <returns>
        /// An observable collection of objects from the endpoint.
        /// </returns>
        public static IObservable<T> Create<T>(Uri uri, IDictionary<string, IObservable<object>> inputStreams, JsonSerializerSettings serializerSettings = null)
        {
            serializerSettings = serializerSettings ?? DefaultSerializerSettings;
            return Observable.Create<T>(
                observer =>
                {
                    var cancellation = new CancellationTokenSource();
                    var subscriptions = new List<IDisposable>(inputStreams.Count);

                    // Connect to the WebSocket and pipe all input streams into it.
                    var socket =
                        WebSocket.Connect(uri, cancellation.Token)
                            .ToObservable()
                            .Do(
                                sock =>
                                {
                                    subscriptions.AddRange(inputStreams.Select(kvp => PipeToObserver(sock, kvp.Value, kvp.Key, serializerSettings)));
                                })
                            .Merge();

                    // Set up the dispose method, which will unwind everything.
                    var subscription = new IDisposable[] { null };
                    Action dispose = () =>
                    {
                        if (subscription[0] != null)
                        {
                            subscription[0].Dispose();
                        }

                        foreach (var sub in subscriptions)
                        {
                            sub.Dispose();
                        }

                        cancellation.Dispose();
                    };

                    // Subscribe the socket to the observer.
                    subscription[0] = SubscribeToSocket(socket, observer, dispose, serializerSettings);
                    return Disposable.Create(dispose);
                });
        }

        /// <summary>
        /// Subscribes the provided <paramref name="observer"/> to the provided <paramref name="socket"/>, returning the subscription.
        /// </summary>
        /// <typeparam name="T">
        /// The underlying stream type.
        /// </typeparam>
        /// <param name="socket">
        /// The socket.
        /// </param>
        /// <param name="observer">
        /// The observer.
        /// </param>
        /// <param name="dispose">
        /// The delegate called on disposal.
        /// </param>
        /// <param name="serializerSettings">
        /// The serializer Settings.
        /// </param>
        /// <returns>
        /// The subscription.
        /// </returns>
        private static IDisposable SubscribeToSocket<T>(IObservable<string> socket, IObserver<T> observer, Action dispose, JsonSerializerSettings serializerSettings)
        {
            return socket.Subscribe(
                next =>
                {
                    try
                    {
                        if (string.IsNullOrEmpty(next))
                        {
                            return;
                        }

                        var type = next[0];
                        switch (type)
                        {
                            case 'n':
                                var value = JsonConvert.DeserializeObject<T>(next.Substring(1), serializerSettings);
                                observer.OnNext(value);
                                break;
                            case 'e':
                                var error = next.Substring(1);
                                observer.OnError(new WebStreamException(error));
                                dispose();
                                break;
                            case 'c':
                                observer.OnCompleted();
                                dispose();
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        observer.OnError(e);
                        dispose();
                    }
                },
                error =>
                {
                    observer.OnError(error);
                    dispose();
                },
                () =>
                {
                    observer.OnCompleted();
                    dispose();
                });
        }

        /// <summary>
        /// Pipes the provided <paramref name="observable"/> into the provided <paramref name="observer"/>.
        /// </summary>
        /// <typeparam name="T">
        /// The type of <see cref="observable"/>.
        /// </typeparam>
        /// <param name="observer">
        /// The observer.
        /// </param>
        /// <param name="observable">
        /// The observable.
        /// </param>
        /// <param name="name">
        /// The name of the observable.
        /// </param>
        /// <param name="serializerSettings">
        /// The serializer Settings.
        /// </param>
        /// <returns>
        /// An <see cref="IDisposable"/> which unwinds the pipe when disposed.
        /// </returns>
        private static IDisposable PipeToObserver<T>(IObserver<string> observer, IObservable<T> observable, string name, JsonSerializerSettings serializerSettings)
        {
            var subscription = new IDisposable[] { null };
            return subscription[0] = observable.Subscribe(
                next =>
                {
                    try
                    {
                        observer.OnNext('n' + name + '.' + JsonConvert.SerializeObject(next, serializerSettings));
                    }
                    catch (Exception e)
                    {
                        observer.OnNext('e' + name + '.' + e.Message);
                        if (subscription[0] != null)
                        {
                            subscription[0].Dispose();
                        }
                    }
                },
                error =>
                {
                    observer.OnNext('e' + name + '.' + error.Message);
                    if (subscription[0] != null)
                    {
                        subscription[0].Dispose();
                    }
                },
                () =>
                {
                    observer.OnNext('c' + name);
                    if (subscription[0] != null)
                    {
                        subscription[0].Dispose();
                    }
                });
        }
    }
}