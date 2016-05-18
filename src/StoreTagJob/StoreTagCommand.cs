using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AppSyndication.BackendModel.Data;

namespace AppSyndication.WebJobs.StoreTagJob
{
    public class StoreTagCommand
    {
        private UpdateStorageCommand _command;
        private ITagQueue _tagQueue;
        private ITagTransactionTable _tagTxTable;

        public StoreTagCommand(ITagTransactionTable transactionTable, ITagQueue tagQueue, UpdateStorageCommand command)
        {
            _tagTxTable = transactionTable;
            _tagQueue = tagQueue;
            _command = command;
        }

        public async Task ExecuteAsync(string channel, string transactionId)
        {
            var tagTx = await _tagTxTable.GetTagTransactionAsync(channel, transactionId);

            if (tagTx == null)
            {
                throw new StoreTagJobException($"Could not find transaction id: {transactionId} in channel: {channel}", true);
            }

            await _command.ExecuteAsync(tagTx);

            await _tagQueue.EnqueueMessageAsync(new IndexChannelMessage(tagTx.Channel));
        }
    }
}
