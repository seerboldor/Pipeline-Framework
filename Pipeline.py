import json

class Pipeline:

    def __init__(self, name=None, function=None, parent=None):
        self.name = name if name is not None else "Unnamed_Pipeline"
        self.function = function
        self.parent = parent  # 父pipeline的引用
        self.tasks = []
        self.task_map = {}
        self.status = "pending"
        self.name_to_task = {}

    def add(self, *tasks):
        for task in tasks:
            if isinstance(task, Pipeline):
                task.parent = self  # 设置父pipeline
            self.tasks.append(task)
            if task.name in self.name_to_task:
                print(f"Warning: Task name '{task.name}' is duplicated.")
            else:
                self.name_to_task[task.name] = task

        # 更新当前pipeline及所有父pipeline的task_map
        self.notify_parents()

    def notify_parents(self):
        self._update_task_map()
        if self.parent:
            self.parent.notify_parents()

    def _update_task_map(self):
        self.task_map = {}
        self._build_task_map(self, [])

    def _build_task_map(self, pipeline, prefix):
        for i, task in enumerate(pipeline.tasks):
            current_prefix = prefix + [i]
            if task.is_pipeline():
                # 递归地更新子pipeline的task_map
                task._update_task_map()  # 添加此行
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

    def execute_by_pointer(self, data, pointer):

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

    def _find_next_pointer_by_name(self, name):
        # 首先，找到具有给定名称的任务的位置
        task_position = None
        for pointer, func in self.task_map.items():
            task_index = json.loads(pointer)[-1]
            task = self.tasks[task_index]  # 使用索引从self.tasks获取任务
            if task.name == name:
                task_position = json.loads(pointer)
                break
        
        if task_position is None:
            print(f"Task with name '{name}' not found in task_map.")  # 调试打印
            return None

        # 然后，找到该位置之后的下一个任务
        next_pointer = self._find_next_pointer(task_position)
        if next_pointer is None:
            print(f"No next pointer found for task '{name}'.")  # 调试打印
        return next_pointer

    def execute_by_name(self, data, name):
        if name in self.name_to_task:
            task = self.name_to_task[name]
            if task.function is None:
                print(f"Task '{name}' does not have an executable function.")
                return None, None
            self.status = "running"  # 更新状态为 running
            output = task.function(data)
            next_pointer = self._find_next_pointer_by_name(name)
            self.status = "finished"  # 更新状态为 finished
            return output, next_pointer
        else:
            for task in self.tasks:
                if task.is_pipeline():
                    result, next_pointer = task.execute_by_name(data, name)
                    if result is not None:
                        return result, next_pointer
            self.status = "closed"  # 如果没有任务执行，设置状态为 closed
            print(f"No task found with name '{name}'.")  # 调试打印
            return None, None


    def __call__(self, *args):
        if len(args) == 2:  # 执行任务
            data, identifier = args
            if isinstance(identifier, list):
                return self.execute_by_pointer(data, identifier)
            elif isinstance(identifier, str):
                return self.execute_by_name(data, identifier)
            else:
                print("Invalid identifier type for task execution.")
                return 
        elif len(args) == 1:
            identifier = args[0]
            if isinstance(identifier, list):  # 根据位置获取 Pipeline 实例
                return self.get_pipeline_at_position(identifier)
            elif isinstance(identifier, str):  # 根据名称获取 Pipeline 实例
                return self.get_pipeline_by_name(identifier)
            else:
                print("Invalid identifier type for accessing pipeline.")
                return 
        else:
            print("Invalid call format.")
            return 

    def get_pipeline_by_name(self, name):
        """根据给定的名称递归地返回对应的Pipeline实例（如果重名则返回第一个）"""
        for task in self.tasks:
            if task.name == name and isinstance(task, Pipeline):
                return task
            elif isinstance(task, Pipeline):
                result = task.get_pipeline_by_name(name)
                if result is not None:
                    return result
        return None


    def get_pipeline_at_position(self, position):
        """根据给定的位置返回对应的Pipeline实例"""
        current_pipeline = self
        for index in position:
            if index >= len(current_pipeline.tasks):
                print(f"No task at position {position}.")
                return None
            current_pipeline = current_pipeline.tasks[index]
            if not isinstance(current_pipeline, Pipeline):
                print(f"Task at position {position} is not a Pipeline.")
                return None
        return current_pipeline

    def display(self, method="node", level=0):
        if method == "tree":
            self._display_tree(level)
        elif method == "node":
            self._display_node()

    def _display_tree(self, level, position=[]):
        indent = "    " * level
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


str_data="Good"

def task1_func(data):
    return f"task1_func {data}"

def task2_func(data):
    return f"task2_func {data}"

def task3_func(data):
    return f"task3_func {data}"

def task4_func(data):
    return f"task4_func {data}"

def task5_func(data):
    return f"task5_func {data}"

task1=Pipeline("task1",task1_func)
task2=Pipeline("task2",task2_func)
task3=Pipeline("task3",task3_func)
task4=Pipeline("task4",task4_func)
task5=Pipeline("task5",task5_func)

process1 = Pipeline()
process2 = Pipeline()

process1.add(task1)
process1.add(process2)
process1.add(task5)

process2.add(task2,task3,task4)


process1.display()

str_a, point =process1(str_data, [1, 0])
print(str_a, point)

str_a, point =process1(str_data, [1, 0])
print(str_a, point)

str_a, point =process1(str_data, "task3")
print(str_a, point)

str_a, point =process1(str_data, "Unnamed_Pipeline")
print(str_a, point)

print(process1([1, 1]).name)

print(process1("task4").parent.name)

