import json
import time  # Adicionar a importação do módulo time
from threading import Thread, Event
from queue import Queue

notebook_list = [
    "dim/scorp_dim_sc_periodo",
    "fact_full/scorp_fact_full_sc_calendario_ocorrencia",
    "fact_full/scorp_fact_full_sc_disciplina"
]

def run_notebook(notebook, error_event):
    print("Executando notebook:", notebook)
    try:
        dbutils.notebook.run(notebook, 0)
        print(f"{notebook} executado com sucesso:")
    except Exception as e:
        print(f"Erro ao executar o notebook {notebook}")
        error_event.set()  # Sinaliza que ocorreu um erro

error_event = Event()

q = Queue()
work_count = 12

for notebook in notebook_list:
    q.put(notebook)

def run_tasks(function, q, error_event):
    while not q.empty():
        value = q.get()
        try_count = 0
        while try_count < 3:  # Limite de 3 tentativas
            run_notebook(value, error_event)
            if not error_event.is_set():
                break
            print(f"Retentativa {try_count+1} para o notebook {value} em 30 segundos...")
            time.sleep(30)  # Intervalo de espera de 30 segundos entre as retentativas
            try_count += 1
        q.task_done()

# Iniciar threads para execução dos notebooks
threads = []
for i in range(work_count):
    t = Thread(target=run_tasks, args=(run_notebook, q, error_event))
    t.daemon = True
    t.start()
    threads.append(t)

# Esperar até que todas as threads terminem
q.join()

# Verificar se ocorreu um erro em algum notebook filho
if error_event.is_set():
    print("Pelo menos um notebook filho falhou. O código pai também irá falhar.")
    raise Exception("Um ou mais notebooks filhos falharam.")
else:
    print("Todos os notebooks filhos foram executados com sucesso.")

# Aguardar todas as threads filho terminarem completamente antes de encerrar o programa
for t in threads:
    t.join()