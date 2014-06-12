// <summary>
//   WebStream methods.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Dapr.WebStreams.Client
{
    using System;
    using System.Collections.Generic;
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
                    var socket = WebSocket.ConnectOutput(uri, cancellation.Token).ToObservable().Merge();
                    SubscribeToSocket(socket, observer, cancellation, serializerSettings);
                    return Disposable.Create(cancellation.Cancel);
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
                    try
                    {
                        // Connect to the WebSocket and pipe all input streams into it.
                        var socket = WebSocket.Connect(uri, cancellation.Token).ToObservable().Do(
                            sock =>
                            {
                                foreach (var input in inputStreams)
                                {
                                    PipeToObserver(sock, input.Value, input.Key, cancellation.Token, serializerSettings);
                                }
                            }).Merge();

                        // Subscribe the socket to the observer.
                        SubscribeToSocket(socket, observer, cancellation, serializerSettings);
                    }
                    catch (Exception e)
                    {
                        observer.OnError(e);
                        cancellation.Cancel();
                    }

                    // Return a disposable which will unwind everything.
                    return Disposable.Create(cancellation.Cancel);
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
        /// <param name="cancellation">
        /// The cancellation token source which is cancelled on error.
        /// </param>
        /// <param name="serializerSettings">
        /// The serializer Settings.
        /// </param>
        private static void SubscribeToSocket<T>(
            IObservable<string> socket,
            IObserver<T> observer,
            CancellationTokenSource cancellation,
            JsonSerializerSettings serializerSettings)
        {
            cancellation.Token.Register(
                socket.Subscribe(
                    next =>
                    {
                        try
                        {
                            if (string.IsNullOrEmpty(next) || cancellation.IsCancellationRequested)
                            {
                                return;
                            }

                            switch (next[0])
                            {
                                case 'n':
                                    var value = JsonConvert.DeserializeObject<T>(next.Substring(1), serializerSettings);
                                    observer.OnNext(value);
                                    break;
                                case 'e':
                                    var error = next.Substring(1);
                                    observer.OnError(new WebStreamException(error));
                                    cancellation.Cancel();
                                    break;
                                case 'c':
                                    observer.OnCompleted();
                                    cancellation.Cancel();
                                    break;
                            }
                        }
                        catch (Exception e)
                        {
                            observer.OnError(e);
                            cancellation.Cancel();
                        }
                    },
                    error =>
                    {
                        observer.OnError(error);
                        cancellation.Cancel();
                    },
                    () =>
                    {
                        observer.OnCompleted();
                        cancellation.Cancel();
                    }).Dispose);
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
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <param name="serializerSettings">
        /// The serializer Settings.
        /// </param>
        private static void PipeToObserver<T>(
            IObserver<string> observer,
            IObservable<T> observable,
            string name,
            CancellationToken cancellationToken,
            JsonSerializerSettings serializerSettings)
        {
            cancellationToken.Register(
                observable.Subscribe(
                    next =>
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        try
                        {
                            observer.OnNext('n' + name + '.' + JsonConvert.SerializeObject(next, serializerSettings));
                        }
                        catch (Exception e)
                        {
                            observer.OnNext('e' + name + '.' + e.Message);
                        }
                    },
                    error => observer.OnNext('e' + name + '.' + error.Message),
                    () => observer.OnNext('c' + name)).Dispose);
        }
    }
}