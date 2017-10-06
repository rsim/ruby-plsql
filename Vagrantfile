# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  # Every Vagrant virtual environment requires a box to build off of.
  config.vm.box = "boxcutter/centos72"
  config.vm.hostname = "vagrant.oracle"
  config.vm.network :forwarded_port, guest: 1521, host: 1521

  config.vm.provider :virtualbox do |vb|
    vb.name = "Ruby-PLSQL Oracle XE box"
    # Speed up network
    #   http://serverfault.com/a/453260/105586
    #   http://serverfault.com/a/595010/105586
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
  end

  # Check for Oracle XE installation file
  config.vm.provision :shell, path: "./spec/support/file_check_script.sh"

  config.vm.provision :shell, inline: "yum update -y"
  config.vm.provision :shell, inline: "yum install -y libaio bc flex unzip"
  config.vm.provision :shell, inline: "mkdir -p /opt/oracle"
  config.vm.provision :shell, inline: "unzip -o -q -d /opt/oracle /vagrant/oracle-xe-11.2.0-1.0.x86_64.rpm.zip"
  config.vm.provision :shell, inline: "cd /opt/oracle/Disk1 && rpm -ivh oracle-xe-11.2.0-1.0.x86_64.rpm"
  config.vm.provision :shell, inline: %q{sed -i -E "s/<value required>/oracle/" /opt/oracle/Disk1/response/xe.rsp}
  config.vm.provision :shell, inline: "/etc/init.d/oracle-xe configure responseFile=/opt/oracle/Disk1/response/xe.rsp >> /opt/oracle/XEsilentinstall.log"
  config.vm.provision :shell, inline: ". /u01/app/oracle/product/11.2.0/xe/bin/oracle_env.sh"
  config.vm.provision :shell, inline: "touch /etc/profile.d/oracle_profile.sh && cat /u01/app/oracle/product/11.2.0/xe/bin/oracle_env.sh >> /etc/profile.d/oracle_profile.sh"
  config.vm.provision :shell, inline: %q{sed -i -E "s/HOST = [^)]+/HOST = $HOSTNAME/g" /u01/app/oracle/product/11.2.0/xe/network/admin/listener.ora}
  config.vm.provision :shell, inline: %q{sed -i -E "s/<ORACLE_BASE>/\/u01\/app\/oracle/" /u01/app/oracle/product/11.2.0/xe/dbs/init.ora}

  # Change password for Oracle user
  config.vm.provision :shell, inline: "echo -e \"oracle\noracle\" | passwd oracle"

end
