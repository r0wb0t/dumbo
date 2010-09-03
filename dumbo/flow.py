import sys
import os
from threading import Thread
from operator import concat
from UserList import UserList
from traceback import print_exc

import dumbo
import dumbo.cmd
from dumbo.util import parseargs, getopt
from dumbo.core import StreamingIteration, UnixIteration

from common.utils import poparg

def override_opts(opts, newopts):
    newopts_keys = set(key for key, val in newopts)

    opts = [(key,val) for key, val in opts if key not in newopts_keys]
    opts += newopts

    return opts

class Result(object):
    def __init__(self, path=None, temporary=False, generated=True):
        self.path = path
        self.generated = generated
        self.temporary = temporary

    @property
    def pattern(self):
        return self.path

class ResultSet(UserList):
    @classmethod
    def from_string(cls, string):
        set = cls()
        for path in string.split(','):
            set.append(Result(path))

        return set

    def __rshift__(self, other):
        if isinstance(other, ResultSet):
            other.generator.inputs = self
            return other

        elif callable(other):
            # Assume it's a mapper for addmulti
            return (self, other)

        else:
            raise NotImplementedError

    @property
    def pattern(self):
        if len(self) != 1:
            raise TypeError("Can only get pattern if resultset contains a single result. It contains %s." % len(self))
        else:
            return self[0].pattern
        

class Iteration(Thread):
    def __init__(self, flow, index, outputs, args, kwargs):
        Thread.__init__(self)
        self.daemon = True

        self.flow = flow

        self.inputs = ResultSet()
        self.outputs = outputs
        self.outputs.generator = self

        self.args = args
        self.kwargs = kwargs

        self.index = index
        self.total = 0

    def __repr__(self):
        return "%s >> %s >> %s" % (
                ','.join(input.path for input in self.inputs),
                self.args[0], 
                ','.join(output.path for output in self.outputs))

    def is_ready(self):
        return all(input.generated for input in self.inputs)

    def run(self):
        try:
            self.launch(*self.args, **self.kwargs)

            #TODO: check if they were actually generated
            for output in self.outputs:
                output.generated = True

            self.exception = None
        except BaseException, e:
            # Exceptions in threads don't cause the interpreter to exit (even SystemExit)
            # Store it for the main thread to handle
            self.exception = e

    def run_task(self):
        self.kwargs['iter'] = self.index

        dumbo.run(*self.args, **self.kwargs)
    
    def launch(self, mapper, reducer=None, combiner=None, opts=None, *args, **kwargs):
        "Copied from dumbo.core.run"
        if not opts:
            opts = []

        if type(mapper) == str:
            opts.append(('mapper', mapper))
        elif hasattr(mapper, 'opts'):
            opts += mapper.opts
        if type(reducer) == str:
            opts.append(('reducer', reducer))
        elif hasattr(reducer, 'opts'):
            opts += reducer.opts
        if type(combiner) == str:
            opts.append(('combiner', combiner))

        opts += [
            ('param', 'FLOW_INPUTS=%s' % ';'.join(self.flow.inputs)),
            ('param', 'FLOW_OUTPUTS=%s' % ';'.join(self.flow.outputs)),
        ]

        opts += self.flow.opts

        opts = override_opts(opts, self.get_connect_opts())

        if not reducer:
            opts.append(('numreducetasks','0'))

        progopt = getopt(opts, 'prog')
        hadoopopt = getopt(opts, 'hadoop', delete=False)
        if hadoopopt:
            retval = StreamingIteration(progopt[0], opts).run()
        else:
            retval = UnixIteration(progopt[0], opts).run()
        if retval == 127:
            print >> sys.stderr, 'ERROR: Are you sure that "python" is on your path?'
        if retval != 0:
            sys.exit(retval)

    def get_connect_opts(self):
        opts = []

        for input in self.inputs:
            opts += [('input', input.path)]

        if any(input.temporary for input in self.inputs):
            opts += [('inputformat', 'code')]

        for output in self.outputs:
            opts += [('output', output.path)]

        if any(output.temporary for output in self.outputs):
            opts += [('outputformat', 'code')]
        
        opts += [
            ('iteration', str(self.index)),
            ('itercount', str(self.total)),
        ]

        return opts

class LocalIteration(Iteration):
    def launch(self, func, *args, **kwargs):
        args = [input.path for input in self.inputs] + \
                [output.path for output in self.outputs] + \
                list(args)
        func(*args, **kwargs)

class DeferredMultiMapper(dumbo.MultiMapper):
    """Multimapper that initializes its submappers at launch

    Instances must have a mappings attribute of pairs of ResultsSet inputs
    and mappers
    """
    def get_inputs(self):
        inputs = set()

        for resultset, mapper in self.mappings:
            inputs |= set(resultset)

        return ResultSet(inputs)

    def configure(self):
        for resultset, mapper in self.mappings:
            for result in resultset:
                self.add(result.pattern, mapper)
    
        super(DeferredMultiMapper, self).configure()

class NonDAGFlow(Exception):
    pass

class Flow(object):
    def __init__(self, opts, inputs, outputs):
        self.opts = opts

        self.waiting = []
        self.running = []

        self.inputs = inputs
        self.outputs = outputs

    def addlocal(self, func, num_outputs=0, delete_outputs=True):
        return self.additer(LocalIteration, num_outputs, delete_outputs, [func], {})

    def addmulti(self, mappings, *args, **kwargs):
        multi = DeferredMultiMapper()
        multi.mappings = mappings

        return multi.get_inputs() >> self.add(multi, *args, **kwargs)

    def add(self, *args, **kwargs):
        num_outputs    = poparg('num_outputs', kwargs, 1)
        delete_outputs = poparg('delete_outputs', kwargs, True)

        return self.additer(Iteration, num_outputs, delete_outputs, args, kwargs)

    def additer(self, cls, num_outputs, delete_outputs, args, kwargs):
        index = len(self.waiting)

        final_output = self.outputs[0]

        outputs = []
        for i in range(num_outputs):
            output_path = '%s_pre_%d_%d' % (final_output, index, i)
            outputs.append(ResultSet([Result(output_path, generated=False, temporary=delete_outputs)]))

        combined_outputs = reduce(concat, outputs, ResultSet())
        iter = cls(self,index,combined_outputs,args,kwargs)
        self.waiting.append(iter)

        if num_outputs < 2:
            return combined_outputs
        else:
            for output in outputs:
                output.generator = iter
            return outputs
        
    def run_task(self, index):
        self.waiting[index].run_task()

    def run_all(self):
        total = len(self.waiting)
        for thread in self.waiting:
            thread.total = total

        self.start_ready()
            
        while self.running:
            for thread in self.running:
                thread.join(1)
                if not thread.is_alive():
                    if thread.exception:
                        raise thread.exception

                    self.running.remove(thread)
                    self.delete_free(thread.inputs)
                    self.start_ready()

        if self.waiting:
            raise NonDAGFlow

    def run_all_sequential(self):
        total = len(self.waiting)
        for thread in self.waiting:
            thread.total = total

        while self.waiting:
            ready = self.pop_ready()
            if len(ready) == 0:
                raise NonDAGFlow

            self.waiting += ready[1:]
            thread = ready[0]
            
            thread.run()
            if thread.exception:
                raise thread.exception
            self.delete_free(thread.inputs)

    def start_ready(self):
        for iter in self.pop_ready():
            iter.start()
            self.running.append(iter)

    def pop_ready(self):
        new_waiting = []
        ready = []

        for iter in self.waiting:
            if iter.is_ready():
                ready.append(iter)
            else:
                new_waiting.append(iter)

        self.waiting = new_waiting
        return ready

    def delete_free(self, inputs):
        for input in inputs:
            in_use = any(input in iter.inputs for iter in self.waiting + self.running)
            if input.temporary and not in_use:
                dumbo.cmd.rm(input.path, self.opts)
                

def main(module=None):
    if module is None:
        import __main__
        module = __main__

    intask = len(sys.argv) > 1 and sys.argv[1][0] != '-'
    opts = parseargs(sys.argv[1:])

    if intask:
        input_paths = os.environ['FLOW_INPUTS'].split(';')
        output_paths = os.environ['FLOW_OUTPUTS'].split(';')
    else:
        sequential = 'yes' in getopt(opts, 'seq')

        input_paths = getopt(opts, 'input')
        output_paths = getopt(opts, 'output')

        if any(';' in path for path in input_paths):
            print >> sys.stderr, "ERROR: Input paths cannot contain semi-colons"
            sys.exit(1)
        if any(';' in path for path in output_paths):
            print >> sys.stderr, "ERROR: Output paths cannot contain semi-colons"
            sys.exit(1)

    if any(',' in path for path in output_paths):
        print >> sys.stderr, "ERROR: Output paths cannot contain commas"
        sys.exit(1)

    print >> sys.stderr, "INFO: Flow inputs: %s" % input_paths
    print >> sys.stderr, "INFO: Flow outputs: %s" % output_paths

    flow = Flow(opts, input_paths, output_paths)

    # call special init function to initialize the flow
    positional_inputs = []
    named_inputs = {}
    
    for path_string in input_paths:
        if '=' in path_string:
            name, value = path_string.split('=',1)
            named_inputs[name] = ResultSet.from_string(value)
        else:
            positional_inputs.append(ResultSet.from_string(path_string))

    outputs = module.init(flow, *positional_inputs, **named_inputs)

    if type(outputs) is ResultSet:
        outputs = [outputs]

    for resultset, path in zip(outputs, output_paths):
        if len(resultset) > 1:
            print >> sys.stderr, "ERROR: Final outputs must be singleton resultsets"
            sys.exit(1)
        output = resultset[0]
        output.path = path
        output.temporary = False

    if intask:
        iterarg = 0
        if len(sys.argv) > 2:
            iterarg = int(sys.argv[2])

        flow.run_task(iterarg)
    else:
        if sequential:
            flow.run_all_sequential()
        else:
            flow.run_all()
                


