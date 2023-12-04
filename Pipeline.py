import json

class Pipeline:

    def __init__(self, name, function=None):
        self.name = name
        self.function = function
        self.tasks = []
        self.task_map = {}
        self.status = "pending"  # 默认状态为 pending

    def add(self, task):
        self.tasks.append(task)
        self._update_task_map()

    def _update_task_map(self):
        self.task_map = {}
        self._build_task_map(self, [])

    def _build_task_map(self, pipeline, prefix):
        for i, task in enumerate(pipeline.tasks):
            current_prefix = prefix + [i]
            if task.is_pipeline():
                self._build_task_map(task, current_prefix)
            elif task.function is not None:
                self.task_map[json.dumps(current_prefix)] = task.function

    def _find_next_pointer(self, pointer):
        pointer_list = [json.loads(k) for k in self.task_map.keys()]
        try:
            current_index = pointer_list.index(pointer)
            if current_index + 1 < len(pointer_list):
                return pointer_list[current_index + 1]
        except ValueError:
            pass
        return None

    def is_pipeline(self):
        return len(self.tasks) > 0

    def execute(self, data, pointer):
        pointer_str = json.dumps(pointer)
        if pointer_str in self.task_map:
            self.status = "running"  # 更新状态为 running
            func = self.task_map[pointer_str]
            output = func(data)
            next_pointer = self._find_next_pointer(pointer)
            self.status = "finished"  # 更新状态为 finished
            return output, next_pointer
        self.status = "closed"  # 如果没有任务执行，设置状态为 closed
        return None, None

    def __call__(self, data, pointer):
        return self.execute(data, pointer)

    def display(self, method="node", level=0):
        if method == "tree":
            self._display_tree(level)
        elif method == "node":
            self._display_node()

    def _display_tree(self, level, position=[]):
        indent = "    " * level  # 用于缩进的空格
        status_str = f"Status: {self.status}"
        print(f"{indent}- {self.name} - {json.dumps(position)} - {status_str}")

        for i, task in enumerate(self.tasks):
            new_position = position + [i]
            if task.is_pipeline():
                task._display_tree(level + 1, new_position)
            else:
                print(f"{indent}    - {task.name} - {json.dumps(new_position)} - Status: {task.status}")

    def _display_node(self, prefix="", position=[]):
        full_name = f"{prefix}{self.name}"
        status_str = f"Status: {self.status}"
        if self.tasks:
            for i, task in enumerate(self.tasks):
                new_position = position + [i]
                if task.is_pipeline():
                    task._display_node(f"{full_name} - ", new_position)
                else:
                    print(f"- {full_name} - {task.name} - {json.dumps(new_position)} - Status: {task.status}")
        else:
            print(f"- {full_name} - {json.dumps(position)} - {status_str}")