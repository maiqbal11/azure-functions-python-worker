import asyncio
import os

from . import bindings
from . import functions
from . import loader
from . import protos

from .context_enabled_task import ContextEnabledTask
from .logging import logger


class Handlers(object):

    async def _handle__worker_init_request(self, disp, req):
        logger.info('Received WorkerInitRequest, request ID %s',
                    disp.request_id)
        return protos.StreamingMessage(
            request_id=disp.request_id,
            worker_init_response=protos.WorkerInitResponse(
                result=protos.StatusResult(
                    status=protos.StatusResult.Success)))

    async def _handle__function_load_request(self, disp, req):
        func_request = req.function_load_request
        function_id = func_request.function_id

        logger.info('Received FunctionLoadRequest, request ID: %s, '
                    'function ID: %s', disp.request_id, function_id)

        try:
            func = loader.load_function(
                func_request.metadata.name,
                func_request.metadata.directory,
                func_request.metadata.script_file,
                func_request.metadata.entry_point)

            disp._functions.add_function(
                function_id, func, func_request.metadata)

            logger.info('Successfully processed FunctionLoadRequest, '
                        'request ID: %s, function ID: %s',
                        disp.request_id, function_id)

            return protos.StreamingMessage(
                request_id=disp.request_id,
                function_load_response=protos.FunctionLoadResponse(
                    function_id=function_id,
                    result=protos.StatusResult(
                        status=protos.StatusResult.Success)))

        except Exception as ex:
            return protos.StreamingMessage(
                request_id=disp.request_id,
                function_load_response=protos.FunctionLoadResponse(
                    function_id=function_id,
                    result=protos.StatusResult(
                        status=protos.StatusResult.Failure,
                        exception=disp._serialize_exception(ex))))

    async def _handle__invocation_request(self, disp, req):
        invoc_request = req.invocation_request

        invocation_id = invoc_request.invocation_id
        function_id = invoc_request.function_id

        # Set the current `invocation_id` to the current task so
        # that our logging handler can find it.
        current_task = asyncio.Task.current_task(disp._loop)
        assert isinstance(current_task, ContextEnabledTask)
        current_task.set_azure_invocation_id(invocation_id)

        logger.info('Received FunctionInvocationRequest, request ID: %s, '
                    'function ID: %s, invocation ID: %s',
                    disp.request_id, function_id, invocation_id)

        try:
            fi: functions.FunctionInfo = disp._functions.get_function(
                function_id)

            args = {}
            for pb in invoc_request.input_data:
                pb_type_info = fi.input_types[pb.name]
                if bindings.is_trigger_binding(pb_type_info.binding_name):
                    trigger_metadata = invoc_request.trigger_metadata
                else:
                    trigger_metadata = None
                args[pb.name] = bindings.from_incoming_proto(
                    pb_type_info.binding_name, pb.data,
                    trigger_metadata=trigger_metadata,
                    pytype=pb_type_info.pytype)

            if fi.requires_context:
                args['context'] = bindings.Context(
                    fi.name, fi.directory, invocation_id)

            if fi.output_types:
                for name in fi.output_types:
                    args[name] = bindings.Out()

            if fi.is_async:
                call_result = await fi.func(**args)
            else:
                call_result = await disp._loop.run_in_executor(
                    disp._sync_call_tp,
                    disp._run_sync_func, invocation_id, fi.func, args)

            if call_result is not None and not fi.has_return:
                raise RuntimeError(
                    f'function {fi.name!r} without a $return binding '
                    f'returned a non-None value')

            output_data = []
            if fi.output_types:
                for out_name, out_type_info in fi.output_types.items():
                    val = args[out_name].get()
                    if val is None:
                        # TODO: is the "Out" parameter optional?
                        # Can "None" be marshaled into protos.TypedData?
                        continue

                    rpc_val = bindings.to_outgoing_proto(
                        out_type_info.binding_name, val,
                        pytype=out_type_info.pytype)
                    assert rpc_val is not None

                    output_data.append(
                        protos.ParameterBinding(
                            name=out_name,
                            data=rpc_val))

            return_value = None
            if fi.return_type is not None:
                return_value = bindings.to_outgoing_proto(
                    fi.return_type.binding_name, call_result,
                    pytype=fi.return_type.pytype)

            logger.info('Successfully processed FunctionInvocationRequest, '
                        'request ID: %s, function ID: %s, invocation ID: %s',
                        disp.request_id, function_id, invocation_id)

            return protos.StreamingMessage(
                request_id=disp.request_id,
                invocation_response=protos.InvocationResponse(
                    invocation_id=invocation_id,
                    return_value=return_value,
                    result=protos.StatusResult(
                        status=protos.StatusResult.Success),
                    output_data=output_data))

        except Exception as ex:
            return protos.StreamingMessage(
                request_id=disp.request_id,
                invocation_response=protos.InvocationResponse(
                    invocation_id=invocation_id,
                    result=protos.StatusResult(
                        status=protos.StatusResult.Failure,
                        exception=disp._serialize_exception(ex))))

    async def _handle__function_environment_reload_request(self, disp, req):
        try:
            logger.info('Received FunctionEnvironmentReloadRequest, '
                        'request ID: %s', disp.request_id)

            func_env_reload_request = req.function_environment_reload_request

            env_vars = func_env_reload_request.environment_variables

            for var in env_vars:
                os.environ[var] = env_vars[var]

            success_response = protos.FunctionEnvironmentReloadResponse(
                result=protos.StatusResult(
                    status=protos.StatusResult.Success))

            return protos.StreamingMessage(
                request_id=disp.request_id,
                function_environment_reload_response=success_response)

        except Exception as ex:
            failure_response = protos.FunctionEnvironmentReloadResponse(
                result=protos.StatusResult(
                    status=protos.StatusResult.Failure,
                    exception=disp._serialize_exception(ex)))

            return protos.StreamingMessage(
                request_id=disp.request_id,
                function_environment_reload_response=failure_response)
