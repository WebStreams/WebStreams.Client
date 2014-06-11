// <summary>
//   A WebStream exception.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Dapr.WebStreams.Client
{
    using System;

    /// <summary>
    /// A WebStream exception.
    /// </summary>
    public class WebStreamException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="WebStreamException"/> class.
        /// </summary>
        /// <param name="message">
        /// The message.
        /// </param>
        /// <param name="innerException">
        /// The inner exception.
        /// </param>
        public WebStreamException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="WebStreamException"/> class.
        /// </summary>
        /// <param name="message">
        /// The message.
        /// </param>
        public WebStreamException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="WebStreamException"/> class.
        /// </summary>
        public WebStreamException()
        {
        }
    }
}
