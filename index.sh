mongo EOS --eval 'db.blocks.createIndex({"block_num": 1},{background: true})'

mongo EOS --eval 'db.transactions.createIndex({"actions.account": 1, "actions.name": 1},{background: true})'

mongo EOS --eval 'db.transactions.createIndex({"block_num": 1},{background: true})'

mongo --port 27018 EOS --eval 'db.transactions.createIndex({"block_num": 1},{background: true})'
