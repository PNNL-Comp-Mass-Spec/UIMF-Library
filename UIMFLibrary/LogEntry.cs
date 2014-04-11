// --------------------------------------------------------------------------------------------------------------------
// <copyright file="LogEntry.cs" company="">
//   
// </copyright>
// <summary>
//   Defines the LogEntry type.
// </summary>
// --------------------------------------------------------------------------------------------------------------------
namespace UIMFLibrary
{
	using System;

	/// <summary>
	/// TODO The log entry.
	/// </summary>
	public class LogEntry
	{
		#region Public Properties

		/// <summary>
		/// Gets or sets the message.
		/// </summary>
		public string Message { get; set; }

		/// <summary>
		/// Gets or sets the posted by.
		/// </summary>
		public string PostedBy { get; set; }

		/// <summary>
		/// Gets or sets the posting time.
		/// </summary>
		public DateTime PostingTime { get; set; }

		/// <summary>
		/// Gets or sets the type.
		/// </summary>
		public string Type { get; set; }

		#endregion
	}
}