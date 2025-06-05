# module5_dispatcher.py

def generate_subtasks(inputs):
    answer1 = inputs["answer1"]
    answer4 = abs(inputs["answer4"])
    base_size = 15000 + int((answer1 + answer4) % 50)
    chunk_size = 3000

    num_chunks = base_size // chunk_size
    subtasks = []

    for i in range(num_chunks):
        for j in range(num_chunks):
            task = {
                "block_row": i,
                "block_col": j,
                "chunk_size": chunk_size,
                "base_size": base_size,
                "answer1": answer1,
                "answer4": answer4
            }
            subtasks.append(task)

    return subtasks