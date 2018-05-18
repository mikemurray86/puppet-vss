Puppet::Type.newtype(:vss) do
  desc "A type to describe how to interact with volume shadow services"
  ensurable
  newparam(:name, :namevar => :true) do
    desc "the drive vss is acting on"
      munge do |value|
        value.upcase
      end
  end
  newparam(:drive_id) do
      desc "The drive ID for the volume being backed up"
  end
  newproperty(:storage_volume) do
      desc "The drive to store the vss copies on"
  end
  newparam(:storage_id) do
      desc "The windows drive ID for the storage drive"
  end
  newproperty(:storage_space) do
      desc "The ammount of space allocated to storing shadow copies represented as a percent"
  end
  newproperty(:schedule) do
      desc "The times that vss snapshots will be taken"
  end
end
