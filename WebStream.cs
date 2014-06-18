// <summary>
//   WebStream methods.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Dapr.WebStreams.Client
{
    using System;
    using System.Collections.Generic;
    using System.Reactive;
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
                    SubscribeLocalToRemote(socket, observer, serializerSettings);
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
                local =>
                {
                    var cancellation = new CancellationTokenSource();
                    try
                    {
                        // Connect to the WebSocket and pipe all input streams into it.
                        var remote = WebSocket.Connect(uri, cancellation.Token).ToObservable().Do(
                            socket =>
                            {
                                foreach (var input in inputStreams)
                                {
                                    var subscription = SubscribeRemoteToLocal(socket, input.Value, input.Key, serializerSettings);
                                    cancellation.Token.Register(()=>subscription.Dispose());
                                }
                            }).Merge();

                        // Subscribe the socket to the observer.
                        var remoteSubscription = SubscribeLocalToRemote(remote, local,  serializerSettings);
                        cancellation.Token.Register(()=>remoteSubscription.Dispose());
                    }
                    catch (Exception e)
                    {
                        local.OnError(e);
                        cancellation.Cancel();
                    }

                    // Return a disposable which will unwind everything.
                    return Disposable.Create(()=>cancellation.Cancel());
                });
        }

        /// <summary>
        /// Subscribes the provided <paramref name="local"/> to the provided <paramref name="remote"/>, returning the subscription.
        /// </summary>
        /// <typeparam name="T">
        /// The underlying stream type.
        /// </typeparam>
        /// <param name="remote">
        /// The socket.
        /// </param>
        /// <param name="local">
        /// The observer.
        /// </param>
        /// <param name="cancellation">
        /// The cancellation token source which is cancelled on error.
        /// </param>
        /// <param name="serializerSettings">
        /// The serializer Settings.
        /// </param>
        /// <returns>
        /// The <see cref="IDisposable"/> subscription.
        /// </returns>
        private static IDisposable SubscribeLocalToRemote<T>(
            IObservable<string> remote,
            IObserver<T> local,
            JsonSerializerSettings serializerSettings)
        {
            return remote.Materialize().Subscribe(
                next =>
                {
                    switch (next.Kind)
                    {
                        case NotificationKind.OnNext:
                            try
                            {
                                var value = next.Value;
                                switch (value[0])
                                {
                                    case 'n':
                                        local.OnNext(JsonConvert.DeserializeObject<T>(value.Substring(1), serializerSettings));
                                        break;
                                    case 'e':
                                        var error = value.Substring(1);
                                        local.OnError(new WebStreamException(error));
                                        break;
                                    case 'c':
                                        local.OnCompleted();
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                local.OnError(new WebStreamException("OnNext failed.", e));
                            }

                            break;
                        case NotificationKind.OnError:
                            local.OnError(next.Exception);
                            break;
                        case NotificationKind.OnCompleted:
                            local.OnCompleted();
                            break;
                    }
                });
        }

        /// <summary>
        /// Pipes the provided <paramref name="local"/> into the provided <paramref name="remote"/>.
        /// </summary>
        /// <typeparam name="T">
        /// The type of <see cref="local"/>.
        /// </typeparam>
        /// <param name="remote">
        /// The observer.
        /// </param>
        /// <param name="local">
        /// The observable.
        /// </param>
        /// <param name="name">
        /// The name of the observable.
        /// </param>
        /// <param name="serializerSettings">
        /// The serializer Settings.
        /// </param>
        /// <returns>
        /// The <see cref="IDisposable"/> subscription.
        /// </returns>
        private static IDisposable SubscribeRemoteToLocal<T>(
            IObserver<string> remote,
            IObservable<T> local,
            string name,
            JsonSerializerSettings serializerSettings)
        {
            return local.Materialize().Subscribe(
                next =>
                {
                    switch (next.Kind)
                    {
                        case NotificationKind.OnCompleted:
                            remote.OnNext('c' + name);
                            break;
                        case NotificationKind.OnError:
                            remote.OnNext('e' + name + '.' + next.Exception.Message);
                            break;
                        case NotificationKind.OnNext:
                            try
                            {
                                var msg = 'n' + name + '.' + JsonConvert.SerializeObject(next.Value, serializerSettings);
                                remote.OnNext(msg);
                            }
                            catch (Exception e)
                            {
                                remote.OnNext('e' + name + '.' + e.Message);
                            }

                            break;
                    }
                });
        }
    }
}