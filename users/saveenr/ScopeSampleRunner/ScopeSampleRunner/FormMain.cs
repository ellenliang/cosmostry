using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using CONAPP=SlavaGu.ConsoleAppLauncher;

namespace ScopeSampleRunner
{
    public partial class FormMain : Form
    {
        public FormMain()
        {
            InitializeComponent();
        }

        private void buttonRun_Click(object sender, EventArgs e)
        {
            string script_filename = @"D:\Scope.StandardDeviation\Scope.StandardDeviation\Scope.script";

            var app = new CONAPP.ConsoleApp(@"d:\scopesdk\scope.exe", "run -i " + script_filename);

            // Create on Observable on the ConsoleOutput Events
            // Use ObserveOn to handle the threading stuff automatically
            var output_events = System.Reactive.Linq.Observable.FromEventPattern<CONAPP.ConsoleOutputEventArgs>(ev => app.ConsoleOutput += ev, ev => app.ConsoleOutput -= ev).ObserveOn(SynchronizationContext.Current);

            // when there is a ConsoleOutput event handle it
            output_events.Subscribe(evt => HandleConsoleOutput(evt));

            // Prepare for running

            this.TextBoxOutput.Clear();

            // Rnn it!
            app.Run();

            
        }

        private void HandleConsoleOutput(EventPattern<CONAPP.ConsoleOutputEventArgs> evt)
        {
            this.TextBoxOutput.AppendText(evt.EventArgs.Line + Environment.NewLine);
        }
    }
}
