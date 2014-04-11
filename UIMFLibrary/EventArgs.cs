// --------------------------------------------------------------------------------------------------------------------
// <copyright file="EventArgs.cs" company="">
//   
// </copyright>
// <summary>
//   Defines the MessageEventArgs type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------
namespace UIMFLibrary
{
	using System;

	/// <summary>
	/// TODO The message event args.
	/// </summary>
	public class MessageEventArgs : EventArgs
	{
		#region Fields

		/// <summary>
		/// TODO The message.
		/// </summary>
		public readonly string Message;

		#endregion

		#region Constructors and Destructors

		/// <summary>
		/// Initializes a new instance of the <see cref="MessageEventArgs"/> class.
		/// </summary>
		/// <param name="message">
		/// TODO The message.
		/// </param>
		public MessageEventArgs(string message)
		{
			this.Message = message;
		}

		#endregion
	}

	/// <summary>
	/// TODO The progress event args.
	/// </summary>
	public class ProgressEventArgs : EventArgs
	{
		#region Fields

		/// <summary>
		/// Value between 0 and 100
		/// </summary>
		public readonly double PercentComplete;

		#endregion

		#region Constructors and Destructors

		/// <summary>
		/// Initializes a new instance of the <see cref="ProgressEventArgs"/> class.
		/// </summary>
		/// <param name="percentComplete">
		/// TODO The percent complete.
		/// </param>
		public ProgressEventArgs(double percentComplete)
		{
			this.PercentComplete = percentComplete;
		}

		#endregion
	}
}