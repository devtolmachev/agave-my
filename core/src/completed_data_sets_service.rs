//! [`CompletedDataSetsService`] is a hub, that runs different operations when a "completed data
//! set", also known as a [`Vec<Entry>`], is received by the validator.
//!
//! Currently, `WindowService` sends [`CompletedDataSetInfo`]s via a `completed_sets_receiver`
//! provided to the [`CompletedDataSetsService`].

use {
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender}, serde_json::{from_str, json, Value}, solana_entry::entry::Entry, solana_ledger::blockstore::{Blockstore, CompletedDataSetInfo}, solana_rpc::{max_slots::MaxSlots, rpc_subscriptions::RpcSubscriptions}, solana_sdk::{signature::Signature, transaction::VersionedTransaction}, std::{
        fs::{self, OpenOptions}, io::Write, sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        }, thread::{self, Builder, JoinHandle}, time::{Duration, SystemTime, UNIX_EPOCH}
    }
};

pub type CompletedDataSetsReceiver = Receiver<Vec<CompletedDataSetInfo>>;
pub type CompletedDataSetsSender = Sender<Vec<CompletedDataSetInfo>>;

pub struct CompletedDataSetsService {
    thread_hdl: JoinHandle<()>,
}

impl CompletedDataSetsService {
    pub fn new(
        completed_sets_receiver: CompletedDataSetsReceiver,
        blockstore: Arc<Blockstore>,
        rpc_subscriptions: Arc<RpcSubscriptions>,
        exit: Arc<AtomicBool>,
        max_slots: Arc<MaxSlots>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("solComplDataSet".to_string())
            .spawn(move || {
                info!("CompletedDataSetsService has started");
                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }
                    if let Err(RecvTimeoutError::Disconnected) = Self::recv_completed_data_sets(
                        &completed_sets_receiver,
                        &blockstore,
                        &rpc_subscriptions,
                        &max_slots,
                    ) {
                        break;
                    }
                }
                info!("CompletedDataSetsService has stopped");
            })
            .unwrap();
        Self { thread_hdl }
    }

    fn recv_completed_data_sets(
        completed_sets_receiver: &CompletedDataSetsReceiver,
        blockstore: &Blockstore,
        rpc_subscriptions: &RpcSubscriptions,
        max_slots: &Arc<MaxSlots>,
    ) -> Result<(), RecvTimeoutError> {
        let completed_data_sets = completed_sets_receiver.recv_timeout(Duration::from_secs(1))?;
        let mut max_slot = 0;
        for completed_set_info in std::iter::once(completed_data_sets)
            .chain(completed_sets_receiver.try_iter())
            .flatten()
        {
            let CompletedDataSetInfo {
                slot,
                start_index,
                end_index,
            } = completed_set_info;
            max_slot = max_slot.max(slot);

            fn add_array_to_json(file_path: &str, new_array: Vec<Value>) -> std::io::Result<()> {
                // Читаем существующий файл
                let mut data = fs::read_to_string(file_path)?;
                let mut json_data = from_str(&data).unwrap_or(json!([]));
            
                // Добавляем новый массив
                if let Some(arr) = json_data.as_array_mut() {
                    arr.extend(new_array);
                }
            
                // Записываем обратно в файл
                let mut file = OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(file_path)?;
                file.write_all(serde_json::to_string(&json_data)?.as_bytes())?;
            
                Ok(())
            }

            match blockstore.get_entries_in_data_block(slot, start_index, end_index, None) {
                Ok(entries) => {
                    let transactions = Self::get_transaction_signatures(entries.clone());
                    if !transactions.is_empty() {
                        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
                        let ts = chrono::Utc::now();
                        let value: Vec<Value> = entries
                            .iter()
                            .flat_map(|x| x.clone().transactions) // Извлекаем все транзакции
                            .map(|tx| json!({"tx": format!("{:?}", tx), "ts": ts})) // Преобразуем каждую транзакцию в Value
                            .collect();
                        info!("completed data sets service - {:?}", value);
                        rpc_subscriptions.notify_signatures_received((slot, transactions));
                    }
                }
                Err(e) => warn!("completed-data-set-service deserialize error: {:?}", e),
            }
        }
        max_slots
            .shred_insert
            .fetch_max(max_slot, Ordering::Relaxed);

        Ok(())
    }

    fn get_transaction_signatures(entries: Vec<Entry>) -> Vec<Signature> {
        entries
            .into_iter()
            .flat_map(|e| {
                e.transactions
                    .into_iter()
                    .filter_map(|mut t| t.signatures.drain(..).next())
            })
            .collect::<Vec<Signature>>()
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
pub mod test {
    use {
        super::*,
        solana_sdk::{
            hash::Hash,
            signature::{Keypair, Signer},
            transaction::Transaction,
        },
    };

    #[test]
    fn test_zero_signatures() {
        let tx = Transaction::new_with_payer(&[], None);
        let entries = vec![Entry::new(&Hash::default(), 1, vec![tx])];
        let signatures = CompletedDataSetsService::get_transaction_signatures(entries);
        assert!(signatures.is_empty());
    }

    #[test]
    fn test_multi_signatures() {
        let kp = Keypair::new();
        let tx =
            Transaction::new_signed_with_payer(&[], Some(&kp.pubkey()), &[&kp], Hash::default());
        let entries = vec![Entry::new(&Hash::default(), 1, vec![tx.clone()])];
        let signatures = CompletedDataSetsService::get_transaction_signatures(entries);
        assert_eq!(signatures.len(), 1);

        let entries = vec![
            Entry::new(&Hash::default(), 1, vec![tx.clone(), tx.clone()]),
            Entry::new(&Hash::default(), 1, vec![tx]),
        ];
        let signatures = CompletedDataSetsService::get_transaction_signatures(entries);
        assert_eq!(signatures.len(), 3);
    }
}
