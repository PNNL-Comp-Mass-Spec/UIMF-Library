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

	public class MessageEventArgs : EventArgs
	{
		public readonly string Message;

		public MessageEventArgs(string message)
		{
			Message = message;
		}
	}

	public class ProgressEventArgs : EventArgs
	{
		/// <summary>
		/// Value between 0 and 100
		/// </summary>
		public readonly double PercentComplete;

		public ProgressEventArgs(double percentComplete)
		{
			PercentComplete = percentComplete;
		}
	}

}
