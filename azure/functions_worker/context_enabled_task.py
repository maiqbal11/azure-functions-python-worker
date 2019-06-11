import asyncio


class ContextEnabledTask(asyncio.Task):

    _AZURE_INVOCATION_ID = '__azure_function_invocation_id__'

    def __init__(self, coro, loop):
        super().__init__(coro, loop=loop)

        current_task = asyncio.Task.current_task(loop)
        if current_task is not None:
            invocation_id = getattr(
                current_task, self._AZURE_INVOCATION_ID, None)
            if invocation_id is not None:
                self.set_azure_invocation_id(invocation_id)

    def set_azure_invocation_id(self, invocation_id):
        setattr(self, self._AZURE_INVOCATION_ID, invocation_id)
