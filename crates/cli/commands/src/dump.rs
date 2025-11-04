//! Re-execute blocks from database in parallel.

use crate::common::{
    AccessRights, CliComponentsBuilder, CliNodeComponents, CliNodeTypes, Environment,
    EnvironmentArgs,
};
use alloy_consensus::{transaction::TxHashRef, BlockHeader, TxReceipt};
use alloy_primitives::map::HashMap;
use alloy_primitives::{Address, B256, U256};
use clap::Parser;
use eyre::WrapErr;
use reth_chainspec::{EthChainSpec, EthereumHardforks, Hardforks};
use reth_cli::chainspec::ChainSpecParser;
use reth_consensus::FullConsensus;
use reth_evm::{execute::Executor, ConfigureEvm};
use reth_primitives_traits::{format_gas_throughput, BlockBody, GotExpected};
use reth_provider::{
    BlockNumReader, BlockReader, ChainSpecProvider, DatabaseProviderFactory, ReceiptProvider,
    StaticFileProviderFactory, TransactionVariant,
};
use reth_revm::database::{EvmStateProvider, StateProviderDatabase};
use reth_revm::db::states::plain_account::PlainStorage;
use reth_revm::db::PlainAccount;
use reth_revm::primitives::KECCAK_EMPTY;
use reth_revm::state::bal::Bal;
use reth_revm::state::{AccountInfo, Bytecode};
use reth_revm::{Database, DatabaseRef};
use reth_stages::stages::calculate_gas_used_from_headers;
use serde::{Deserialize, Serialize};
use std::any::type_name;
use std::collections::BTreeMap;
use std::path::Path;
use std::{
    fs::File,
    io::{Read, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinSet};
use tracing::*;

fn type_of<T>(_: &T) -> &'static str {
    type_name::<T>()
}

/// `reth re-execute` command
///
/// Re-execute blocks in parallel to verify historical sync correctness.
#[derive(Debug, Parser)]
pub struct DumpCommand<C: ChainSpecParser> {
    #[command(flatten)]
    env: EnvironmentArgs<C>,

    /// The height to start at.
    #[arg(long, default_value = "1")]
    from: u64,

    /// The height to end at. Defaults to the latest block.
    #[arg(long)]
    to: Option<u64>,
}

impl<C: ChainSpecParser> DumpCommand<C> {
    /// Returns the underlying chain being used to run this command
    pub fn chain_spec(&self) -> Option<&Arc<C::ChainSpec>> {
        Some(&self.env.chain)
    }
}

impl<C: ChainSpecParser<ChainSpec: EthChainSpec + Hardforks + EthereumHardforks>> DumpCommand<C> {
    /// Execute `re-execute` command
    pub async fn execute<N>(self, components: impl CliComponentsBuilder<N>) -> eyre::Result<()>
    where
        N: CliNodeTypes<ChainSpec = C::ChainSpec>,
    {
        let Environment { provider_factory, .. } = self.env.init::<N>(AccessRights::RO)?;

        let provider = provider_factory.database_provider_ro()?;
        let components = components(provider_factory.chain_spec());

        let min_block = self.from;
        let max_block = self.to.unwrap_or(provider.best_block_number()?);
        println!(
            "Start dumping bals, blockHashes, prestates, blocks from block:{} to block:{}, total blocks:{}",
            min_block, max_block, max_block-min_block
        );

        let db_at = {
            let provider_factory = provider_factory.clone();
            move |block_number: u64| {
                StateProviderDatabase(
                    provider_factory.history_by_block_number(block_number).unwrap(),
                )
            }
        };

        // let input_bals = read_bals_from_file();
        // let input_bals = Arc::new(Mutex::new(input_bals));
        // let bals: Arc<Mutex<std::collections::HashMap<u64, Option<reth_revm::state::bal::Bal>>>> = Arc::new(Mutex::new(HashMap::new()));

        // Spawn thread executing blocks
        let provider_factory = provider_factory.clone();
        let evm_config = components.evm_config().clone();
        let db_at = db_at.clone();

        // export blocks
        let mut blocks = vec![];
        for block in min_block..max_block {
            let bn = block;
            let block = provider_factory
                .recovered_block(block.into(), TransactionVariant::NoHash)?
                .unwrap();
            let block = block.into_block();
            blocks.push(block);
        }

        // execute and export pre-block states, bals
        let mut prestates = Vec::with_capacity((max_block - min_block) as usize);
        let mut bals = Vec::with_capacity((max_block - min_block) as usize);
        for bn in min_block..max_block {
            let cur_db = db_at(bn - 1);
            let mut executor = evm_config.batch_executor(cur_db);
            let block =
                provider_factory.recovered_block(bn.into(), TransactionVariant::NoHash)?.unwrap();
            let result = executor.execute_one(&block)?;
            let bal_block = result.bal.unwrap();

            // bals
            bals.push(bal_block.clone());
            // prestates
            let cur_db = db_at(bn - 1);
            let preblock_state = readset_from_bal(bal_block, cur_db);

            prestates.push(preblock_state);
        }

        let mut block_hashes = BTreeMap::new();
        let latest_db = db_at(max_block);
        for bn in min_block - 257..max_block {
            let block_hash = latest_db.block_hash_ref(bn).unwrap();
            block_hashes.insert(bn, block_hash);
        }

        let nblocks = max_block - min_block;
        let bal_file = format!("bals_{}.json", nblocks);
        write_data(&bal_file, bals);
        let prestate_file = format!("prestates_{}.json", nblocks);
        write_data(&prestate_file, prestates);
        let block_file = format!("blocks_{}.json", nblocks);
        write_data(&block_file, blocks);
        let block_hashes_file = format!("blockHashes_{}.json", nblocks);
        write_data(&block_hashes_file, block_hashes);
        println!("write blocks:{min_block}-{max_block}, blocks:{block_file}, bals:{bal_file}, pre-block states:{prestate_file}, blockHashes:{block_hashes_file}");

        Ok(())
    }
}

fn write_data<T, U: AsRef<Path>>(filename: U, data: T)
where
    T: serde::Serialize,
{
    let fs = File::create(filename).unwrap();
    serde_json::to_writer(fs, &data).unwrap();
}

// fn read_bals_from_file() -> HashMap<u64, Option<reth_revm::state::bal::Bal>> {
//     let mut file = File::open("bals.json").ok().unwrap();
//     let mut contents = String::new();
//     file.read_to_string(&mut contents).ok().unwrap();

//     serde_json::from_str(&contents).unwrap()
// }

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MyPlainAccount {
    pub info: Option<AccountInfo>,
    /// Account storage.
    pub storage: PlainStorage,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PreblockState {
    // code is in acct info
    pub accounts: HashMap<Address, MyPlainAccount>,
}

fn readset_from_bal<DB: EvmStateProvider>(
    bal: Bal,
    provider: StateProviderDatabase<DB>,
) -> PreblockState {
    let mut state_reads = PreblockState::default();
    for (addr, acct_bal) in bal.accounts {
        let mut acct_info = provider.basic_ref(addr).unwrap();
        let mut storage = PlainStorage::default();
        let storage_bal = acct_bal.storage.storage;
        if let Some(info) = &mut acct_info {
            if info.code_hash != KECCAK_EMPTY {
                let code = provider.code_by_hash_ref(info.code_hash).ok();
                info.code = code;
            }
        }
        for (slot, _) in storage_bal {
            let slot_val = provider.storage_ref(addr, slot).unwrap();
            storage.insert(slot, slot_val);
        }

        let acct = MyPlainAccount { info: acct_info, storage };
        state_reads.accounts.insert(addr, acct);
    }

    state_reads
}
