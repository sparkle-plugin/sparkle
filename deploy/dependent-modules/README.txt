
Step 1: Edit spark-inventory.yml and add the correct machines
under the [my_nodes] heading, one per line.

Step 2: Edit spark-vars.yml and add any additional software packages
you want to install.

Step 3: Install Ansible

	sudo yum install ansible

If the 'ansible' package is not available in the YUM
repositories configured for your machine, you may need
to add the EPEL repository.

On a CentOS machine, for example, this is done as follows:

	export http_proxy='http://YOUR_PROXY_SERVER:PORT'
	curl  http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-6.noarch.rpm > epel-release-7-6.noarch.rpm
	sudo rpm -iUvh epel-release-7-6.noarch.rpm

At this point, you should be able to install the 'ansible' package.

If your machine accesses the Internet via a proxy server, you will
need also to configure YUM to use the proxy.  Edit the file
/etc/yum.conf and add a line

	proxy=http://YOUR_PROXY_SERVER:PORT

Step 4: Set up passwordless SSH connection to machines to be installed.

If you don't have a HOME/.ssh/id_rsa file for your account,
you need to create it via the command:

	ssh-keygen

Select HOME/.ssh/id_rsa as file to be generated, and empty password.

If you already have a HOME/.ssh/id_rsa file, you may need to remove the
passphrase from it:

	ssh-keygen -p -P ''

Once the HOME/.ssh/id_rsa exists and the passphrase is removed, you
need to add its associated public key to the list of authorized keys
on all the machines where you will install Spark4TM:

	cat ~/.ssh/id_rsa.pub | ssh USER@MACHINE 'cat >> .ssh/authorized_keys' 

Step 5: Install pre-requisites

First, edit prereqs-install.yml and set the account name that will be used
to log into the machines to be installed. This is the remote_user
variable at the top of the file.

Then run the command to install the pre-requisites on all the
machines you added to spark-inventory.yml:

	ansible-playbook -i spark-inventory.yml prereqs-install.yml

Step 6: Install Boost

Run the script on machines where Boost is to be installed:

	sudo ./install-boost.sh

Note: You may need to run the 'wget' command by hand in case the connection
to SourceForge has server-side problems.

	wget https://sourceforge.net/projects/boost/files/boost/1.56.0/boost_1_56_0.tar.gz
	mv boost_1_56_0.tar.gz /tmp
	sudo ./install-boost.sh


