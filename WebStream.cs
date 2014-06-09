// <summary>
//   WebStream methods.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace WebStream.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reactive.Disposables;
    using System.Reactive.Linq;
    using System.Reactive.Threading.Tasks;
    using System.Reflection;
    using System.Text;
    using System.Threading;

    using Dapr.WebSockets;

    using Newtonsoft.Json;

    /// <summary>
    /// WebStream methods.
    /// </summary>
    public static class WebStream
    {
        /// <summary>
        /// Connects to the WebSocket specified by <see cref="uri"/> and returns an observable collection of objects
        /// from the endpoint.
        /// </summary>
        /// <param name="uri">
        /// The URI.
        /// </param>
        /// <typeparam name="T">
        /// The underlying type returned by the endpoint.
        /// </typeparam>
        /// <returns>
        /// An observable collection of objects from the endpoint.
        /// </returns>
        public static IObservable<T> Create<T>(Uri uri)
        {
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

                    var socket = ReactiveWebSocket.ConnectOutput(uri, cancellation.Token).ToObservable();
                    subscription[0] = SubscribeToSocket(socket.SelectMany(_ => _), observer, dispose);
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
        /// <param name="parameters">
        /// The name-value collection of simple parameters and input streams to pipe to the endpoint.
        /// </param>
        /// <typeparam name="T">
        /// The underlying type returned by the endpoint.
        /// </typeparam>
        /// <returns>
        /// An observable collection of objects from the endpoint.
        /// </returns>
        public static IObservable<T> Create<T>(Uri uri, IDictionary<string, object> parameters)
        {
            var splitParams = parameters.Where(kvp => kvp.Value != null).GroupBy(group => IsObservableType(group.Key));
            // ReSharper disable once PossibleMultipleEnumeration
            var inputStreams = splitParams.Where(group => group.Key)
                .SelectMany(_ => _)
                .ToDictionary(group => group.Key, group => group.Value as IObservable<object>);
            var builder = new UriBuilder(uri);
            // ReSharper disable once PossibleMultipleEnumeration
            var queryParams = splitParams.Where(group => !@group.Key).SelectMany(_ => _);
            var queryToAppend = CreateQuery(queryParams);

            if (builder.Query != null && builder.Query.Length > 1)
            {
                builder.Query = builder.Query.Substring(1) + "&" + queryToAppend;
            }
            else
            {
                builder.Query = queryToAppend.ToString();
            }

            return Create<T>(builder.Uri, inputStreams);
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
        /// <typeparam name="T">
        /// The underlying type returned by the endpoint.
        /// </typeparam>
        /// <returns>
        /// An observable collection of objects from the endpoint.
        /// </returns>
        public static IObservable<T> Create<T>(Uri uri, IDictionary<string, IObservable<object>> inputStreams)
        {
            return Observable.Create<T>(
                observer =>
                {
                    var cancellation = new CancellationTokenSource();
                    var subscriptions = new List<IDisposable>(inputStreams.Count);

                    // Connect to the WebSocket and pipe all input streams into it.
                    var socket =
                        ReactiveWebSocket.Connect(uri, cancellation.Token)
                            .ToObservable()
                            .Do(sock => subscriptions.AddRange(inputStreams.Select(kvp => PipeToObserver(sock, kvp.Value, kvp.Key))));

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
                    subscription[0] = SubscribeToSocket(socket.SelectMany(_ => _), observer, dispose);
                    return Disposable.Create(dispose);
                });
        }

        private static IDisposable SubscribeToSocket<T>(IObservable<string> socket, IObserver<T> observer, Action dispose)
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
                                var value = JsonConvert.DeserializeObject<T>(next.Substring(1));
                                observer.OnNext(value);
                                break;
                            case 'e':
                                var error = next.Substring(1);
                                observer.OnError(new Exception(error));
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
                });
        }

        /// <summary>
        /// Pipes the provided <paramref name="observable"/> into the provided <paramref name="observer"/>.
        /// </summary>
        /// <typeparam name="T">The type of <see cref="observable"/>.</typeparam>
        /// <param name="observer">The observer.</param>
        /// <param name="observable">The observable.</param>
        /// <param name="name">The name of the observable.</param>
        /// <returns>An <see cref="IDisposable"/> which unwinds the pipe when disposed.</returns>
        private static IDisposable PipeToObserver<T>(IObserver<string> observer, IObservable<T> observable, string name)
        {
            var subscription = new IDisposable[] { null };
            return subscription[0] = observable.Subscribe(
                next =>
                {
                    try
                    {
                        observer.OnNext('n' + name + '.' + JsonConvert.SerializeObject(next));
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

        /// <summary>
        /// Returns <see langword="true"/> if <paramref name="obj"/> is assignable to an observable stream of
        /// strings, <see langword="false"/> otherwise.
        /// </summary>
        /// <param name="obj">The object</param>
        /// <returns>
        /// <see langword="true"/> if <paramref name="obj"/> is assignable to an observable stream of
        /// strings, <see langword="false"/> otherwise.
        /// </returns>
        private static bool IsObservableType(object obj)
        {
            return typeof(IObservable<object>).GetTypeInfo().IsAssignableFrom(obj.GetType().GetTypeInfo());
        }

        /// <summary>
        /// Returns a query string constructed from the provided <paramref name="queryParams"/>.
        /// </summary>
        /// <param name="queryParams">The query parameters.</param>
        /// <returns>
        /// A query string constructed from the provided <paramref name="queryParams"/>.
        /// </returns>
        private static StringBuilder CreateQuery(IEnumerable<KeyValuePair<string, object>> queryParams)
        {
            var queryToAppend = new StringBuilder();
            foreach (var param in queryParams)
            {
                var key = Uri.EscapeUriString(param.Key);
                string value;
                if (param.Value is string)
                {
                    value = param.Value as string;
                }
                else
                {
                    value = JsonConvert.SerializeObject(param.Value);
                }

                queryToAppend.AppendFormat("{0}={1}", key, value);
            }

            return queryToAppend;
        }
    }
}