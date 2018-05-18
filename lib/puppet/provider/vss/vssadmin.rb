require 'puppet/util/windows/taskscheduler'

Puppet::Type.type(:vss).provide(:vssadmin) do
  desc "This implements vssadmin commands to manage VSS settings on a node."

  confine :operatingsystem    => :windows
  defaultfor :operatingsystem => :windows


  commands :vssadmin_exe => "vssadmin.exe"

  mk_resource_methods

  # set up the @property_flush hash to be used later on to sync all
  #   settings that are not configured properly.
  def initialize(value={})
      super(value)
      @property_flush = {}
  end

  # This collects data on storage in use and allocated for VSS use.
  #   As well as the current schedule that new snapshots are taken.
  #
  # @return [Array[Hash]] An array of hashes with the name of the drive,
  #   it's storage drive and the storage space allocated as a percent and
  #   the schedule snapshots are taken.
  def self.collect_storage_info(drive = :all)
    begin
        storage = vssadmin_exe(['list', 'shadowstorage']).split("\n\n")[1]
    rescue Puppet::ExecutionFailure => e
        Puppet.debug("# Execution Failed -> #{e.inspect}")
        return nil
    end
    info = Array.new
    hash = Hash.new
    storage.split("\n").each do |line|
      case line.strip
      when /^For/
        if hash.has_key?(:name)
          hash.clear # if the key is there this is a new resource. clear the old values
        end
        hash[:provider] = :vssadmin
        hash[:name] = line[16]
        hash[:drive_id] = line.split("{")[1][0..-3]
        hash[:schedule] = begin
                            vss_schedule("ShadowCopyVolume{#{hash[:drive_id]}}.job")
                          rescue Puppet::Util::Windows::Error
                              "absent"
                          end
      when /^Shadow .*:/
        hash[:storage_volume] = line.split(":")[1][2]
        hash[:storage_id] = line.split("{")[1][0..-3]
        hash[:ensure] = hash.has_key?(:storage_volume) ? 'present' : 'absent'
      when /^Maximum/
        hash[:storage_space] = "#{line.split("(")[1][0]}%"
        if drive == :all || hash[:name] == drive
          info << hash # this is the last line of data for a resource so push it into the array
        end
      end
    end
    info
  end

  # This is from the built in scheduled_task type and is used to connect to an existing task.
  #   Currently it will only grab tasks that are already managed by puppet.
  #
  # @param task_name [String] the name of a task to activate. the task must have the '.job'
  #   extension
  #
  # @return [Win32::TaskScheduler] An instance connected to the specified task
  def self.vss_task(task_name)
    return @vss_task if @vss_task
    @vss_task ||= Win32::TaskScheduler.new
    @vss_task.activate(task_name) # if exists?
    @vss_task
  end

  # This is also from the built in scheduled_task type and does all the heavy lifting for finding
  #   the schedule.
  #
  # @param name [String] the name of the scheduled task to collect the schedule from.
  # @return [Hash] a hash of all the scheduling information in the same formate used in
  #   scheduled_task.
  def self.vss_schedule(name)
    return @triggers if @triggers

    @triggers   = []
    vss_task(name).trigger_count.times do |i|
      trigger = begin
                  vss_task(name).trigger(i)
                rescue Win32::TaskScheduler::Error
                  # Win32::TaskScheduler can't handle all of the
                  # trigger types Windows uses, so we need to skip the
                  # unhandled types to prevent "puppet resource" from
                  # blowing up.
                  nil
                end
      next unless trigger and scheduler_trigger_types.include?(trigger['trigger_type'])
      puppet_trigger = {}
      case trigger['trigger_type']
      when Win32::TaskScheduler::TASK_TIME_TRIGGER_DAILY
        puppet_trigger['schedule'] = 'daily'
        puppet_trigger['every']    = trigger['type']['days_interval'].to_s
      when Win32::TaskScheduler::TASK_TIME_TRIGGER_WEEKLY
        puppet_trigger['schedule']    = 'weekly'
        puppet_trigger['every']       = trigger['type']['weeks_interval'].to_s
        puppet_trigger['day_of_week'] = days_of_week_from_bitfield(trigger['type']['days_of_week'])
      when Win32::TaskScheduler::TASK_TIME_TRIGGER_MONTHLYDATE
        puppet_trigger['schedule'] = 'monthly'
        puppet_trigger['months']   = months_from_bitfield(trigger['type']['months'])
        puppet_trigger['on']       = days_from_bitfield(trigger['type']['days'])
      when Win32::TaskScheduler::TASK_TIME_TRIGGER_MONTHLYDOW
        puppet_trigger['schedule']         = 'monthly'
        puppet_trigger['months']           = months_from_bitfield(trigger['type']['months'])
        puppet_trigger['which_occurrence'] = occurrence_constant_to_name(trigger['type']['weeks'])
        puppet_trigger['day_of_week']      = days_of_week_from_bitfield(trigger['type']['days_of_week'])
      when Win32::TaskScheduler::TASK_TIME_TRIGGER_ONCE
        puppet_trigger['schedule'] = 'once'
      end
      puppet_trigger['start_date'] = normalized_date("#{trigger['start_year']}-#{trigger['start_month']}-#{trigger['start_day']}")
      puppet_trigger['start_time'] = normalized_time("#{trigger['start_hour']}:#{trigger['start_minute']}")
      puppet_trigger['enabled']    = trigger['flags'] & Win32::TaskScheduler::TASK_TRIGGER_FLAG_DISABLED == 0
      puppet_trigger['minutes_interval'] = trigger['minutes_interval'] ||= 0
      puppet_trigger['minutes_duration'] = trigger['minutes_duration'] ||= 0
      puppet_trigger['index']      = i

      @triggers << puppet_trigger
    end

    @triggers
  end

  # Used to collect all instances of vss for each drive on the system. Currently the schedule
  #   parameter is not returned unless it is already managed from puppet
  #
  # @return [Array] an array of all vss resources
  def self.instances
    collect_storage_info.collect do |info|
      new(info)
    end
  end

  # Used to collect all instances of vss for each drive on the system. Currently the schedule
  #   parameter is not returned unless it is already managed from puppet
  #
  # @return [Array] an array of all vss resources
  def self.prefetch(resources)
      instances.each do |prov|
          if resource = resources[prov.name]
              resource.provider = prov
          end
      end
  end

  # Wrapper to set a value for flush to use
  def create
    @property_flush[:ensure] = 'present'
  end

  # Tests if the resource already exists or not
  def exists?
    @property_hash[:ensure] == 'present'
  end

  # Wrapper to set a value for flush to use
  def destroy
    @property_flush[:ensure] = 'absent'
  end

  # method to set storage settings properly
  def set_storage
    size = if @property_flush[:ensure] == 'absent' then
             '0'
           else
             @property_hash[:storage_space]
           end
  vssadmin_exe('resize',
               'shadowstorage',
               "/For=#{@property_hash[:name]}",
               "/ON=#{@property_hash[:storage_volume]}",
               "/MaxSize=#{size}%" )
  end

  # method to setup the vss schedule
  #   this was taken from the scheduled_task builtin type
  # @param a hash representing the requested schedule
  def set_schedule(value)
    desired_triggers = value.is_a?(Array) ? value : [value]
    current_triggers = trigger.is_a?(Array) ? trigger : [trigger]

    extra_triggers = []
    desired_to_search = desired_triggers.dup
    current_triggers.each do |current|
      if found = desired_to_search.find {|desired| triggers_same?(current, desired)}
        desired_to_search.delete(found)
      else
        extra_triggers << current['index']
      end
    end

    needed_triggers = []
    current_to_search = current_triggers.dup
    desired_triggers.each do |desired|
      if found = current_to_search.find {|current| triggers_same?(current, desired)}
        current_to_search.delete(found)
      else
          needed_triggers << desired unless @property_flush[:ensure] == 'absent'
      end
    end

    extra_triggers.reverse_each do |index|
      vss_task.delete_trigger(index)
    end

    needed_triggers.each do |trigger_hash|
      # Even though this is an assignment, the API for
      # Win32::TaskScheduler ends up appending this trigger to the
      # list of triggers for the task, while #add_trigger is only able
      # to replace existing triggers. *shrug*
      vss_task.trigger = translate_hash_to_trigger(trigger_hash)
    end
  end
  # the method to handle modifing the system.
  #   a call is made to set_storage to ensure storage is configured properly.
  #   then set_schedule is called to adjust the scheduled tasks. this makes use
  #   of code from the builtin resource to handle everything.
  #   finally the @property_hash is updated with the new values
  def flush
    set_storage
    set_schedule(@property_hash[:schedule])

    @property_hash = self.class.collect_storage_info(resource[:name])
  end

  # A collection of flags to work with the schedule functions. This is taken from the
  #   scheduled_task built in type
  def self.normalized_time(time_string)
    Time.parse("#{time_string}").strftime('%H:%M')
  end

  # A collection of flags to work with the schedule functions. This is taken from the
  #   scheduled_task built in type
  def self.normalized_date(date_string)
    date = Date.parse("#{date_string}")
    "#{date.year}-#{date.month}-#{date.day}"
  end

  # A collection of flags to work with the schedule functions. This is taken from the
  #   scheduled_task built in type
  def self.scheduler_trigger_types
    [
      Win32::TaskScheduler::TASK_TIME_TRIGGER_DAILY,
      Win32::TaskScheduler::TASK_TIME_TRIGGER_WEEKLY,
      Win32::TaskScheduler::TASK_TIME_TRIGGER_MONTHLYDATE,
      Win32::TaskScheduler::TASK_TIME_TRIGGER_MONTHLYDOW,
      Win32::TaskScheduler::TASK_TIME_TRIGGER_ONCE
    ]
  end

  # A collection of flags to work with the schedule functions. This is taken from the
  #   scheduled_task built in type
  def self.scheduler_days_of_week
    [
      Win32::TaskScheduler::SUNDAY,
      Win32::TaskScheduler::MONDAY,
      Win32::TaskScheduler::TUESDAY,
      Win32::TaskScheduler::WEDNESDAY,
      Win32::TaskScheduler::THURSDAY,
      Win32::TaskScheduler::FRIDAY,
      Win32::TaskScheduler::SATURDAY
    ]
  end

  # A collection of flags to work with the schedule functions. This is taken from the
  #   scheduled_task built in type
  def self.scheduler_months
    [
      Win32::TaskScheduler::JANUARY,
      Win32::TaskScheduler::FEBRUARY,
      Win32::TaskScheduler::MARCH,
      Win32::TaskScheduler::APRIL,
      Win32::TaskScheduler::MAY,
      Win32::TaskScheduler::JUNE,
      Win32::TaskScheduler::JULY,
      Win32::TaskScheduler::AUGUST,
      Win32::TaskScheduler::SEPTEMBER,
      Win32::TaskScheduler::OCTOBER,
      Win32::TaskScheduler::NOVEMBER,
      Win32::TaskScheduler::DECEMBER
    ]
  end

  # A collection of flags to work with the schedule functions. This is taken from the
  #   scheduled_task built in type
  def self.scheduler_occurrences
    [
      Win32::TaskScheduler::FIRST_WEEK,
      Win32::TaskScheduler::SECOND_WEEK,
      Win32::TaskScheduler::THIRD_WEEK,
      Win32::TaskScheduler::FOURTH_WEEK,
      Win32::TaskScheduler::LAST_WEEK
    ]
  end

  # A collection of flags to work with the schedule functions. This is taken from the
  #   scheduled_task built in type
  def self.day_of_week_constant_to_name(constant)
    case constant
    when Win32::TaskScheduler::SUNDAY;    'sun'
    when Win32::TaskScheduler::MONDAY;    'mon'
    when Win32::TaskScheduler::TUESDAY;   'tues'
    when Win32::TaskScheduler::WEDNESDAY; 'wed'
    when Win32::TaskScheduler::THURSDAY;  'thurs'
    when Win32::TaskScheduler::FRIDAY;    'fri'
    when Win32::TaskScheduler::SATURDAY;  'sat'
    end
  end

  # A collection of flags to work with the schedule functions. This is taken from the
  #   scheduled_task built in type
  def self.day_of_week_name_to_constant(name)
    case name
    when 'sun';   Win32::TaskScheduler::SUNDAY
    when 'mon';   Win32::TaskScheduler::MONDAY
    when 'tues';  Win32::TaskScheduler::TUESDAY
    when 'wed';   Win32::TaskScheduler::WEDNESDAY
    when 'thurs'; Win32::TaskScheduler::THURSDAY
    when 'fri';   Win32::TaskScheduler::FRIDAY
    when 'sat';   Win32::TaskScheduler::SATURDAY
    end
  end

  # a conversion utility from the schedule_task built in type
  def self.month_constant_to_number(constant)
    month_num = 1
    while constant >> month_num - 1 > 1
      month_num += 1
    end
    month_num
  end

  # A collection of flags to work with the schedule functions. This is taken from the
  #   scheduled_task built in type
  def self.occurrence_constant_to_name(constant)
    case constant
    when Win32::TaskScheduler::FIRST_WEEK;  'first'
    when Win32::TaskScheduler::SECOND_WEEK; 'second'
    when Win32::TaskScheduler::THIRD_WEEK;  'third'
    when Win32::TaskScheduler::FOURTH_WEEK; 'fourth'
    when Win32::TaskScheduler::LAST_WEEK;   'last'
    end
  end

  # A collection of flags to work with the schedule functions. This is taken from the
  #   scheduled_task built in type
  def self.occurrence_name_to_constant(name)
    case name
    when 'first';  Win32::TaskScheduler::FIRST_WEEK
    when 'second'; Win32::TaskScheduler::SECOND_WEEK
    when 'third';  Win32::TaskScheduler::THIRD_WEEK
    when 'fourth'; Win32::TaskScheduler::FOURTH_WEEK
    when 'last';   Win32::TaskScheduler::LAST_WEEK
    end
  end
end
