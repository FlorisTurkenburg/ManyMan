"""
ManyMan - A Many-core Visualization and Management System
Copyright (C) 2012
University of Amsterdam - Computer Systems Architecture
Jimi van der Woning and Roy Bakker

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

from communicator import Communicator
from core import Core
from valueslider import ValueSlider
from infopopup import InfoPopup
from kivy.app import App
from kivy.clock import Clock
from kivy.config import Config
from kivy.logger import Logger, LOG_LEVELS
from kivy.uix.accordion import Accordion, AccordionItem
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.image import Image
from kivy.uix.label import Label
from kivy.uix.popup import Popup
from kivy.uix.scrollview import ScrollView
from kivy.uix.widget import WidgetException
from os import _exit as exit
from os.path import exists
from perfgraph import PerfGraph
from task import CoreTask, PendingTask
from time import sleep
from util import is_prime
from widgets import MyTextInput, MyVKeyboard
import config
import kivy
import sys
import task

default_settings = {
    'kivy_version': '1.2.0',
    'keyboards_folder': 'keyboards',
    'logging_level': 'info',
    'address': ['sccsa.science.uva.nl', 11111],
    'framerate': 60.,
    'bufsize': 1024,
    'tasks': [
        {'name': 'Hello World', 'command': '/shared/bakkerr/jimivdw/tests/hello'},
        {'name': 'Simple Counter', 'command': '/shared/bakkerr/jimivdw/tests/count'},
        {
            'name': 'Memory Intensive (100 MB)',
            'command': '/shared/bakkerr/jimivdw/tests/mem 100 100'
        },
        {
            'name': 'Memory Intensive (10 MB)',
            'command': '/shared/bakkerr/jimivdw/tests/mem 10 1000'
        },
 
        {'name': 'Pi estimator', 'command': '/shared/bakkerr/jimivdw/tests/pi 100000000'}
    ],
    'core_background': 'atlas://img/atlas/core',
    'core_background_active': 'atlas://img/atlas/core_active',
    'core_border': [14, 14, 14, 14],
    'core_padding': 9,
    'core_color_range': [.35, 0.],
    'task_default_color': .7,
    'task_info_image': 'atlas://img/atlas/info',
    'task_dup_image': 'atlas://img/atlas/duplicate',
    'task_start_image': 'atlas://img/atlas/play',
    'task_stop_image': 'atlas://img/atlas/stop_button',
    'task_pause_image': 'atlas://img/atlas/pause_button',
    'task_resume_image': 'atlas://img/atlas/play_button',
    'task_move_image': 'atlas://img/atlas/move_button',
    'logo_image': 'img/uva-logo.jpg',
    'help_image': 'img/help.png',
    'about_image': 'img/about.png',
    'license_image': 'img/license.png',
    'output_buffer_size': 100,
    'output_to_file': True,
    'output_folder': 'output',
    'perfgraph_default_history': '50',
    'voltage_islands': [
        [0, 1, 2, 3, 12, 13, 14, 15],
        [4, 5, 6, 7, 16, 17, 18, 19],
        [8, 9, 10, 11, 20, 21, 22, 23],
        [24, 25, 26, 27, 36, 37, 38, 39],
        [28, 29, 30, 31, 40, 41, 42, 43],
        [32, 33, 34, 35, 44, 45, 46, 47]
    ]
}


class ManyMan(App):
    """
    Application window. Contains all visualization and sets up the entire
    front-end system.
    """

    def __init__(self, **kwargs):
        self.settings_file = 'settings.cfg'
        if len(sys.argv) > 1:
            self.settings_file = sys.argv[1]

        self.settings = default_settings.copy()
        self.comm = None
        self.cores = dict()
        self.tasks = dict()
        self.pending_tasks = dict()
        self.pending_count = 0
        self.finished_tasks = dict()
        self.chip_name = ""
        self.chip_cores = ""
        self.chip_orientation = None
        self.started = False

        self.layout = None
        self.sidebar = None
        self.leftbar = None
        self.rightbar = None
        self.task_list = None
        self.finished_list = None
        self.task_create = None
        self.name_input = None
        self.command_input = None
        self.cpu_graph = None
        self.power_graph = None
        self.help_window = None
        self.frequencies_window = None
        self.frequency_labels = []
        self.frequency_sliders = []

        self.load_settings()
        self.config_kivy()
        self.config_logger()
        self.init_communicator()

        super(ManyMan, self).__init__(**kwargs)

    def load_settings(self):
        """Load settings from settings file."""
        try:
            self.settings.update(config.Config(file(self.settings_file)))
        except Exception, err:
            print 'Settings could not be loaded: %s' % err
            exit(1)

    def config_kivy(self):
        """Configure kivy."""
        kivy.require(self.settings['kivy_version'])
        if not exists(self.settings['keyboards_folder']):
            raise WidgetException(
                "Keyboards folder (%s) could not be found." % \
                self.settings['keyboards_folder']
            )
        if Config.get('kivy', 'keyboard_mode') != 'multi' or \
            Config.get('kivy', 'keyboard_layout') != 'qwerty':
            Config.set('kivy', 'keyboard_mode', 'multi')
            Config.set('kivy', 'keyboard_layout', 'qwerty')
            Config.write()
            raise WidgetException(
                "Keyboard mode was not set properly. Need to restart."
            )

    def config_logger(self):
        """Configure the kivy logger."""
        Logger.setLevel(LOG_LEVELS[self.settings['logging_level']])

    def init_communicator(self):
        """Initialize the communicator."""
        try:
            self.comm = Communicator(self)
            self.comm.start()
            while not self.comm.initialized:
                sleep(.1)
        except:
            if self.comm:
                self.comm.running = False
                self.comm.join()
            exit(0)

    def build_config(self, *largs):
        """Copy the settings to the Kivy Config module."""
        Config.setdefaults('settings', self.settings)

    def build(self):
        """Build the main window."""
        Clock.max_iteration = 100
        self.layout = BoxLayout()
        return self.layout

    def on_start(self):
        """Handler when the tool is started."""
        self.set_vkeyboard()
        self.init_leftbar()
        self.init_core_grid()
        self.init_rightbar()
        self.init_task_create()
        self.started = True

    def on_stop(self):
        """Handler when the tool is stopped."""
        self.comm.running = False
        self.comm.join()

    def set_vkeyboard(self):
        """Setup the virtual keyboard."""
        win = self.layout.get_root_window()
        if not win:
            raise WidgetException("Could not access the root window")

        win.set_vkeyboard_class(MyVKeyboard)

    def init_leftbar(self):
        """Initialize the left sidebar."""
        self.leftbar = BoxLayout(
            orientation='vertical',
            padding=5,
            spacing=10,
            size_hint_x=None,
            width=300
        )
        self.init_chip_info()
        self.init_left_controls()
        self.init_pending_list()
        self.init_cpu_graph()

        self.layout.add_widget(self.leftbar)

    def init_core_grid(self):
        """Initialize the core grid on the left side of the window."""
        if self.chip_orientation:
            orientation = self.chip_orientation
            cols = len(self.chip_orientation[0])
        else:
            # Generate the default core orientation
            elems = self.chip_cores
            if is_prime(elems) and elems > 2:
                elems += 1
            rows = 1
            cols = elems
            for i in xrange(1, elems):
                if elems % i == 0 and rows < cols:
                    rows = i
                    cols = elems / i

            orientation = []
            for r in range(rows):
                row = []
                for c in range(cols):
                    row.append(r * cols + c)
                orientation.append(row)

        core_grid = GridLayout(cols=cols, spacing=4)

        for row in orientation:
            for i in row:
                c = Core(i, self)
                self.cores[i] = c
                core_grid.add_widget(c)
                Clock.schedule_interval(
                    c.update,
                    1.0 / self.settings['framerate']
                )

        self.layout.add_widget(core_grid)

    def init_rightbar(self):
        """Initialize the right sidebar."""
        self.rightbar = BoxLayout(
            orientation='vertical',
            padding=5,
            spacing=10,
            size_hint_x=None,
            width=300
        )
        self.init_program_info()
        self.init_right_controls()
        self.init_finished_list()
        
        b = Button(
            text='Set frequency',
            size_hint_y=None,
            height=40
        )
        b.bind(on_press=self.show_frequencies)
        self.rightbar.add_widget(b)
        
        self.init_power_graph()

        self.layout.add_widget(self.rightbar)

    def init_chip_info(self):
        """Initialize the chip information text on the left top corner."""
        chip_info = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            height=60
        )

        chip_info.add_widget(Label(text=self.chip_name, font_size=(20)))
        chip_info.add_widget(Label(
            text=("%d-core chip" % self.chip_cores),
            size_hint=(1, .3)
        ))

        self.leftbar.add_widget(chip_info)

    def init_left_controls(self):
        """Initialize the control buttons below the chip info."""
        controls = GridLayout(
            cols=1,
            spacing=5,
            size_hint_y=None,
            height=40
        )

        task_button = Button(text='Add task')
        task_button.bind(on_press=self.create_task)
        controls.add_widget(task_button)

        self.leftbar.add_widget(controls)

    def init_pending_list(self):
        """Initialize the list of pending tasks."""
        label = Label(
            text="Pending tasks:",
            size_hint_y=None,
            height=20
        )
        self.leftbar.add_widget(label)

        scroll = ScrollView(do_scroll_x=False)

        self.task_list = GridLayout(cols=1, spacing=5, size_hint_y=None)
        self.task_list.bind(minimum_height=self.task_list.setter('height'))

        for task in self.settings['tasks']:
            if 'name' in task:
                self.new_task(task['command'], task['name'])
            else:
                self.new_task(task['command'])

        scroll.add_widget(self.task_list)
        self.leftbar.add_widget(scroll)

    def init_cpu_graph(self):
        """Initialize the chip performance graph in the lower left corner."""
        graph_container = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            height=250
        )
        graph_container.add_widget(Label(
            text="Overall CPU-usage:",
            size_hint_y=None,
            height=30
        ))
        self.cpu_graph = PerfGraph("CPU", color=[.3, 1, 1], history=100)
        graph_container.add_widget(self.cpu_graph)
        self.leftbar.add_widget(graph_container)

    def init_program_info(self):
        """Initialize the program information text on the right top corner."""
        logo = FloatLayout(size_hint=(None, None), size=(290, 60))
        logo.add_widget(
            Image(
                source=self.settings['logo_image'],
                size_hint=(60. / 290., 1),
                pos_hint={'x': .8, 'y': 0}
            )
        )
        self.rightbar.add_widget(logo)

        program_info = BoxLayout(
            orientation='vertical',
            size_hint=(1, 1),
            pos_hint={'x': 0, 'y': 0}
        )

        program_info.add_widget(Label(text="ManyMan", font_size=(20)))
        program_info.add_widget(Label(
            text="Many-core Manager",
            size_hint=(1, .3)
        ))

        logo.add_widget(program_info)

    def init_right_controls(self):
        """Initialize the control buttons below the program info."""
        controls = GridLayout(
            cols=2,
            spacing=5,
            size_hint_y=None,
            height=40
        )

        help_button = Button(text='Help')
        help_button.bind(on_press=self.show_help)
        controls.add_widget(help_button)

        exit_button = Button(text='Exit')
        exit_button.bind(on_press=self.stop)
        controls.add_widget(exit_button)

        self.rightbar.add_widget(controls)

    def init_finished_list(self):
        """Initialize the list of finished tasks."""
        label = Label(
            text="Finished tasks:",
            size_hint_y=None,
            height=20
        )
        self.rightbar.add_widget(label)

        scroll = ScrollView(do_scroll_x=False)

        self.finished_list = GridLayout(cols=1, spacing=5, size_hint_y=None)
        self.finished_list.bind(
            minimum_height=self.finished_list.setter('height')
        )

        scroll.add_widget(self.finished_list)
        self.rightbar.add_widget(scroll)

    def init_power_graph(self):
        """Initialize the chip power graph in the lower right corner."""
        graph_container = BoxLayout(
            orientation='vertical',
            size_hint_y=None,
            height=250
        )
        graph_container.add_widget(Label(
            text="Overall Power-usage:",
            size_hint_y=None,
            height=30
        ))
        self.power_graph = PerfGraph(
            "Power",
            unit="W",
            percent_scale=False,
            color=[.3, 1, 1],
            history=100
        )
        graph_container.add_widget(self.power_graph)
        self.rightbar.add_widget(graph_container)

    def init_task_create(self):
        """Initialize the 'Add task' popup."""
        self.task_create = Popup(
            title="Add task",
            size_hint=(None, None),
            size=(600, 230)
        )

        content = GridLayout(cols=1, spacing=20)

        inputs = FloatLayout(orientation='horizontal')
        inputs.add_widget(
            Label(
                text='Name (optional):',
                text_size=(150, None),
                padding_x=5,
                size_hint=(.25, None),
                height=30,
                pos_hint={'x': 0, 'y': 0}
            )
        )
        self.name_input = MyTextInput(
            multiline=False,
            size_hint=(.75, None),
            height=30,
            pos_hint={'x': .25, 'y': 0}
        )
        inputs.add_widget(self.name_input)
        content.add_widget(inputs)

        inputs = FloatLayout(orientation='horizontal')
        inputs.add_widget(
            Label(
                text='Command:',
                text_size=(150, None),
                padding_x=5,
                size_hint=(.25, None),
                height=30,
                pos_hint={'x': 0, 'y': 0}
            )
        )
        self.command_input = MyTextInput(
            multiline=False,
            size_hint=(.75, None),
            height=30,
            pos_hint={'x': .25, 'y': 0}
        )
        inputs.add_widget(self.command_input)
        content.add_widget(inputs)

        submit = Button(text='Create', size_hint=(1, None), height=30)
        submit.bind(on_press=self.process_task_create)
        content.add_widget(submit)
        self.task_create.content = content

    def create_task(self, *largs):
        """Handler when the 'Add task' button is pressed."""
        self.task_create.open()

    def process_task_create(self, *largs):
        """
        Handler when the 'Create task' button in the 'Add task' popup is
        pressed.
        """
        if self.command_input.text:
            self.new_task(self.command_input.text, self.name_input.text)
        self.task_create.dismiss()
        self.name_input.text = ''
        self.command_input.text = ''

    def new_task(self, command, name=None):
        """Add a new task to the pending tasks list."""
        if not name:
            # Generate a name from the command
            name = command.split()[0].split('/')[-1].capitalize() \
                .replace('_', ' ')

        Logger.debug("ManyMan: Adding task %s (%s)" % (name, command))
        t = PendingTask(
            name.capitalize(),
            "P%04d" % self.pending_count,
            self,
            command=command,
            size_hint=(None, None),
            size=(290, 60)
        )
        self.pending_tasks[t.tid] = t
        self.pending_count += 1
        self.task_list.add_widget(t)

    def has_task(self, tid):
        """Return whether a task with the given tid exists or not."""
        return tid in self.tasks

    def add_task(self, tid, name, cid, status):
        """
        Add a task with the given Task ID (tid) and name to the core with index
        cid. When cid is negative, move the task to the pending tasks list.
        """
        if cid < 0:
            # Move the task to the pending tasks list.
            t = PendingTask(
                name,
                tid,
                self,
                size_hint=(None, None),
                size=(290, 60)
            )
            self.pending_tasks[t.tid] = t
            self.pending_count += 1
            self.task_list.add_widget(t)
            core = self.cores.get(0)
        elif status in ("Finished", "Failed"):
            Logger.info("ManyMan: Encountered finished task %s" % tid)
            core = self.cores.get(0)
        else:
            # Start the task on the supplied core.
            Logger.debug(
                "ManyMan: Adding task %s (%s) to core %d" % (name, tid, cid)
            )
            core = self.cores.get(cid)
            core.text = core.info_text()
            if not core.info_built:
                # Render the core information popup when this is not done yet.
                core.build_info()

        if status == "Finished":
            color = .3
        elif status == "Failed":
            color = 0
        else:
            color = self.settings['task_default_color']

        task = CoreTask(
            name,
            tid,
            core,
            status,
            text=name,
            color=color,
            size_hint=(None, None),
            size=(300, 60)
        )
        if cid < 0:
            task.core = None
        if status in ("Finished", "Failed"):
            task.core = None
            self.finished_tasks[tid] = task
            self.finished_list.add_widget(task)
        self.tasks[tid] = task
        if cid >= 0 and not status in ("Finished", "Failed"):
            core.add_task(task)

        return task

    def move_task(self, task, core=None):
        """
        Move the task with Task ID 'task' to the core with given id 'core'.
        When core is not provided, move the task to the list of pending tasks.
        """
        if core:
            Logger.debug(
                "ManyMan: Moving task %s (%s) to core %d" % \
                (task.name, task.tid, core.index)
            )

        if task.core:
            task.core.remove_task(task)
        else:
            self.pending_tasks.pop(task.tid)
        task.core = core
        if core:
            # Move the task to the supplied core.
            if not core.info_built:
                # Render the core information popup when this is not done yet.
                core.build_info()
            core.add_task(task)
        else:
            # Move the task to the pending tasks list.
            t = PendingTask(
                task.name,
                task.tid,
                self,
                size_hint=(None, None),
                size=(290, 60)
            )
            self.pending_tasks[t.tid] = t
            self.pending_count += 1
            self.task_list.add_widget(t)

    def finish_task(self, tid, status):
        """Move the task with given TID to the finished tasks list."""
        task = self.tasks.get(tid)
        task.status = status
        task.core.remove_task(task)
        task.core = None
        if status == "Finished":
            task.hue = .3
        else:
            task.hue = 0
        self.finished_tasks[tid] = task
        self.finished_list.add_widget(task)
        

    def remove_task(self, tid):
        """Remove the task with given ID 'tid' from the system."""
        task = self.tasks.pop(tid)
        Logger.debug(
            "ManyMan: Removing task %s (%s)" % (task.name, task.tid)
        )
        task.core.remove_task(task)

    def show_help(self, *largs):
        """Show the help popup."""
        if not self.help_window:
            # Build the help window when this has not been done yet.
            self.help_window = Popup(
                title="Help",
                size_hint=(None, None),
                size=(600, 600)
            )

            acc = Accordion()
            self.help_window.content = acc

            item = AccordionItem(title='Help')
            item.add_widget(Image(source=self.settings['help_image']))
            acc.add_widget(item)

            item2 = AccordionItem(title='About')
            item2.add_widget(Image(source=self.settings['about_image']))
            acc.add_widget(item2)

            item3 = AccordionItem(title='License')
            item3.add_widget(Image(source=self.settings['license_image']))
            acc.add_widget(item3)

        self.help_window.open()

    def show_frequencies(self, *largs):
        """Show the frequencies popup."""
        if not self.frequencies_window:
            # Build the help window when this has not been done yet.
            self.frequencies_window = InfoPopup(
                title="Tile frequencies",
                size_hint=(None, None),
                size=(600, 450)
            )

            b = BoxLayout(orientation='vertical')
            self.frequencies_window.content = b

            for i in xrange(len(self.settings['voltage_islands'])):
                r = BoxLayout(size_hint_y=None, height=50)
                l = Label(
                    text='Power domain %d:\nFrequency: %dMHz' % (i, 533),
                    size_hint_x=None,
                    width=150
                )
                self.frequency_labels += [l] 
                r.add_widget(l)
                
                vs = ValueSlider(
                    min=100,
                    max=800,
                    value=533,
                    values=[800, 533, 400, 320, 267, 200, 100],
                    data=i
                )
                vs.val = 533
                vs.bind(on_change=self.frequency_changed)
                vs.bind(on_release=self.frequency_set)
                self.frequency_sliders += [vs]
                r.add_widget(vs)
                b.add_widget(r)

            r = BoxLayout(size_hint_y=None, height=50)
            l = Label(
                text='All power domains:\nFrequency: %dMHz' % (533),
                size_hint_x=None,
                width=150
            )
            self.frequency_labels += [l] 
            r.add_widget(l)
            
            vs = ValueSlider(
                min=100,
                max=800,
                value=533,
                values=[800, 533, 400, 320, 267, 200, 100]
            )
            vs.val = 533
            vs.bind(on_change=self.frequency_changed)
            vs.bind(on_release=self.frequency_set)
            self.frequency_sliders += [vs]
            r.add_widget(vs)
            b.add_widget(r)

        self.frequencies_window.show()

    def frequency_changed(self, ins):
        Logger.debug("ManyMan: slider %s changed" % ins.data)
        if ins.data != None:
            l = self.frequency_labels[ins.data]
            l.text = 'Power domain %d:\nFrequency: %dMHz' % (ins.data, ins.val)
        else:
            l = self.frequency_labels[len(self.settings['voltage_islands'])]
            l.text = 'All power domains:\nFrequency: %dMHz' % (ins.val)
            for i in xrange(len(self.settings['voltage_islands'])):
                s = self.frequency_sliders[i]
                s.val = ins.val

    def frequency_set(self, ins):
        Logger.debug("ManyMan: slider %s set" % ins.data)
        if ins.data != None:
            self.comm.set_core_frequency(
                ins.val,
                self.settings['voltage_islands'][ins.data][0]
            )
        else:
            Logger.info("ManyMan: ssaasdsslider %s set" % ins.data)
            self.comm.set_core_frequency(ins.val)

    def get_cpu_load(self):
        """Getter for the current CPU load. MAY NOT BE CALLED."""
        raise Exception("Can not access the current CPU load.")

    def set_cpu_load(self, value):
        """Setter for the current CPU load. Updates the performance graph."""
        self.cpu_graph.update(value * 100)

    def get_cpu_power(self):
        """Getter for the current CPU power. MAY NOT BE CALLED."""
        raise Exception("Can not access the current CPU power.")

    def set_cpu_power(self, value):
        """Setter for the current CPU power. Updates the performance graph."""
        self.power_graph.update(value)

    # Define getters and setters.
    cpu_load = property(get_cpu_load, set_cpu_load)
    cpu_power = property(get_cpu_power, set_cpu_power)


if __name__ in ('__android__', '__main__'):
    # Run the program.
    manyman = ManyMan()
    try:
        manyman.run()
    except Exception, e:
        Logger.critical("Main: %s" % e)
    finally:
        exit(0)
