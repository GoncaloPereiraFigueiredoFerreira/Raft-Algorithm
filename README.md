# Raft-Algorithm
Our group's implementation of the raft algorithm

# Testes Realizados
teste para verificar replicação do estado -> 

./maelstrom test -w lin-kv --time-limit 20 --node-count 7 --bin ../Raft-Algorithm/raftUltimateVersion.py --concurrency 2n --latency 25 --latency-dist uniform --rate 10 

latencia fixa e sem partiçoes permite ver que o algoritemo em condiçoes otimas e que nao tem que alterar o lider faz replicaçao do seu estado nos outros nodos 


teste para verificar alteraçoes de liderança -> 

./maelstrom test -w lin-kv --time-limit 20 --node-count 7 --bin ../Raft-Algorithm/raftUltimateVersion.py --concurrency 2n --latency 40  --latency-dist exponential --rate 10 

neste teste e intruduzida latencia significava no teste, para verificar a funcinalidade de troca do lider em falta de mensagens de heartbeat


teste para verificar resiliencia a queda de nodos -> 


./maelstrom test -w lin-kv --time-limit 20 --node-count 7 --bin ../Raft-Algorithm/raftUltimateVersion.py --concurrency 2n --latency 25 --latency-dist uniform --rate 10 --nemesis partition --nemesis-interval 10

neste teste a latencia e reduzida de forma a se tonar insegnificante, sao intruduzidas partiçoes na rede, para simular falha de nodos, de forma a provar que o algoritemo e resistente a falhas.
Dado que o tipo das partiçoes e a duraçao das mesmas e aleatorio podera ser nesseçario repetir o testes para conseguir condiçoes desejaveis para o mesmos
