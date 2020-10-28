#!/bin/bash

# This is just a dump of LogDevice-specific code formerly in 'lab' and
# its predecessor 'awld'.

# LogDevice server ports. These will be gradually moved to lab modules
declare -r                   \
	PORT_LD_CLIENT=16001 \
	PORT_LD_GOSSIP=16002 \
	PORT_LD_SSL=         \
	PORT_LD_ADMIN=16004


# Expects $1 to contain comma-separated words. Outputs the same string, but
# with double quotes around every word.
quote_words() {
    echo "\"$1\"" | sed 's/,/","/g'
}


# Get the number of shards provisioned on the specified LogDevice node.
# The node is expected to be up and running
#
# @param $1   hostname of the node
get_nshards() {
    local hostname=$1

    timeout -k $SSH_KILL_AFTER $SSH_TIMEOUT \
	    ssh -qnT $SSHOPTS ubuntu@$hostname cat $DATADIR/NSHARDS
}


# Output a possibly empty list of comma-separated LogDevice roles
# (sequencer,storage) that an EC2 instance of the sepcified type can play.
#
# @param $1   EC2 instance type
get_logdevice_roles_for_instance_type() {
    local itype=$1

    for h in ${!INSTANCE_TYPES[@]}; do
	for t in ${INSTANCE_TYPES[$h]}; do
	    if [[ $t == $itype ]]; then
		case $h in
		    ssd|hdd) echo "sequencer,storage" ; return ;;
		    cpu) echo "sequencer" ; return ;;
		esac
	    fi
	done
    done
}


# Read lines from stdin in the format of list() containing
# configuration details for nodes in a LogDevice cluster, and output
# the following sections of the LogDevice config file: "nodes", "internal_logs",
# and "metadata_logs", separated by commas.
#
# @param $1  : cluster name (NOT including logdevice/ prefix)
nodes_and_internal_config() {
    local cluster_name=$1
    local sep=   # JSON record separator
    local nid=0  # LogDevice id of next node
    local -A ids_by_rack=()        # loc => (<node-ids>) map
    declare -r nodes_max=512 # maximum cluster size

    echo '  "nodes": ['

    local cn iid itype loc ip hostname roles rest
    local storage_state sequencer_state

    while read -r cn iid itype loc ip hostname rest; do
	if [[ $cn != logdevice/$cluster_name ]]; then
	    abort "invalid cluster name."\
		  "Expected 'logdevice/$cluster_name'. Got '$cn'."
	fi
	if [[ $nid -ge $nodes_max ]]; then
	    exit_error "Cluster $cluster_name has too many nodes. "\
		       "Limit is $nodes_max."
	fi
	nshards=$(get_nshards $hostname)
	if [[ $? -ne 0 || "$nshards" -lt 1 ]]; then
	    exit_error "Failed to get the number of LogDevice data shards, "\
		       "or data shards are not set up on instance "\
		       "$iid ($hostname)"
	fi
	roles=$(get_logdevice_roles_for_instance_type $itype)
	if [[ "$roles" =~ sequencer ]]; then
	    sequencer_state='"sequencer": true,'
	else
	    sequencer_state=
	fi
	if [[ "$roles" =~ storage ]]; then
	    storage_state='"storage": "read-write","storage_weight": 1,'
	else
	    storage_state=
	fi
	if [[ $PORT_LD_SSL -gt 0 ]]; then
	    ssl_port="\"ssl_port\": $PORT_LD_SSL,"
	else
	    ssl_port=
	fi
	ids_by_rack[$loc]="${ids_by_rack[$loc]-} $nid"
	cat <<EOF
$sep{
	"node_id": $nid,
	"generation": 1,
	"host": "$ip:$PORT_LD_CLIENT",
 	"gossip_port": $PORT_LD_GOSSIP, $ssl_port
	"location": "$loc",
        "roles": [ $(quote_words $roles) ],
    	$sequencer_state
        $storage_state
        "num_shards": $nshards,
        "ec2-instance": "$iid"
}
EOF
	sep=','
	((nid+=1))
    done

    node_count=$nid
    rack_count=${#ids_by_rack[@]}

    if [[ $node_count -lt 1 ]]; then
	abort "invalid node count $node_count"
    fi
    if [[ $rack_count -lt 1 ]]; then
	abort "invalid rack count $rack_count"
    fi

    # cross-rack replication is not supported yet

    if [[ $node_count -lt $INTERNAL_LOGS_NODESET_SIZE ]]; then
	internal_nodeset_size=$node_count
	>&2 echo "WARNING: INTERNAL_LOGS_NODESET_SIZE"\
	    "($INTERNAL_LOGS_NODESET_SIZE) exceeds cluster size"\
	    "($node_count). Reducing nodeset size to $node_count."\
	    "This will adversely affect cluster's ability to withstand"\
	    "node failures."
    else
	internal_nodeset_size=$INTERNAL_LOGS_NODESET_SIZE
    fi

    if [[ $node_count -le $INTERNAL_LOGS_COPYSET_SIZE ]]; then
	if [[ $node_count -le 1 ]]; then
	    internal_copyset_size=1
	else
	    internal_copyset_size=$(($node_count-1))
	fi
	>&2 echo "WARNING: INTERNAL_LOGS_COPYSET_SIZE"\
	    "($INTERNAL_LOGS_COPYSET_SIZE) equals or exceeds cluster size"\
	    "($node_count). Reducing copyset size of internal logs to"\
	    "$internal_copyset_size. This will adversely affect cluster's"\
	    "ability to withstand node failures."

    else
	internal_copyset_size=$INTERNAL_LOGS_COPYSET_SIZE
    fi

    if [[ $node_count -le $METADATA_LOGS_COPYSET_SIZE ]]; then
	if [[ $node_count -le 1 ]]; then
	    metadata_copyset_size=1
	else
	    metadata_copyset_size=$(($node_count-1))
	fi
	>&2 echo "WARNING: METADATA_LOGS_COPYSET_SIZE"\
	    "($METADATA_LOGS_COPYSET_SIZE) equals or exceeds cluster size"\
	    "($node_count). Reducing copyset size of metadata logs to"\
	    "$metadata_copyset_size. This will adversely affect cluster's"\
	    "ability to withstand node failures."
    else
	metadata_copyset_size=$METADATA_LOGS_COPYSET_SIZE
    fi

    internal_replicate="\"node\": $internal_copyset_size"
    metadata_nodeset=$(seq -s , 0 $(($internal_nodeset_size-1)))

    echo '],'
    cat <<EOF
  "internal_logs": {
     "config_log_deltas": {
         "replicate_across": {
             $internal_replicate
         }
     },
     "config_log_snapshots": {
         "replicate_across": {
             $internal_replicate
         }
     },
     "event_log_deltas": {
         "replicate_across": {
             $internal_replicate
         }
     },
     "event_log_snapshots": {
         "replicate_across": {
             $internal_replicate
         }
     }
  },
  "metadata_logs": {
        "nodeset": [$metadata_nodeset],
        "replicate_across": { "node": $metadata_copyset_size }
  }
EOF
}


# Read the output of list() | grep ^[zk] from stdin and output a string of
# comma separated <private-ip>:<zk-client-port> entries suitable for use
# by Zookeeper clients.
#
# Set $? to 0 if the number of input entries equals the number of Zookeeper
# instance ids in ${SERVICE[zk]} (all privisioned ZK instances are running).
# Otherwise set $? to 1.
# zk_client_config()  -TODO: delete


# Read lines from stdout in the format of list() (see usage()) containing
# configuration details for nodes in a LogDevice cluster, and output a
# cluster config for that cluster.
#
# @param $1 name      : LogDevice cluster name
# @param $2 zookeeper : zookeeper config output by zk_client_config()
cluster_config() {
    local name=$1
    local zookeeper=$2

    for i in name zookeeper; do
	>&2 test -z "${!i}" && abort "empty '$i' parameter"
    done

    cat <<EOF
{
  "client_settings": {},
  "server_settings": {
     "user": "logdevice"
  },
  "cluster": "$name",
  $(nodes_and_internal_config $name),
  "traffic_shaping": {},
  "version": $(date +%s),
  "zookeeper": {
     "timeout": "$ZK_CLIENT_TIMEOUT",
     "quorum": [ $(quote_words $zookeeper) ]
  }
}
EOF
}


# Run zkCli.sh over ssh on a Zookeeper node whose public ip is in $1.
# Pipe stdin to that zkCli.sh process over ssh. Propagate stdout, stderr, and
# exit status of zkCli.sh.
#
# @param $1  zkhost       Hostname or ip of a host in the Zookeeper ensemble
#                         on which to run the command.
zkcli() {
    local zkhost="$1"

    timeout -k $SSH_KILL_AFTER $SSH_TIMEOUT \
	    ssh -qT $SSHOPTS ec2-user@$zkhost \
	/opt/zookeeper/bin/zkCli.sh -server localhost:$ZKCLTPORT
}


configure() {
    [[ ${1-} == help ]] && cat <<'EOF' && return
  $FUNCNAME

  create and store, or retrieve a cluster configuration file. The file
  is stored in a znode on a Zookeeper ensemble previously started by
  start-zk. The znode path is /conf/<cluster-id>.conf

  FLAGS
    -c|--cluster <cluster-id>   REQUIRED  cluster id of cluster to configure

    -s|--show      Do not create a new config. Instead request the
                   cluster config from Zookeeper and print it to stdout.
    --force        Force storing a new config in Zookeeper even if
                   /conf/<cluster-id>.conf znode already exists.
    --dryrun       Generate a config and output to stdout. Do not save.
EOF
    local cluster=
    local show=
    local force=
    local dry=

    while [[ $# -gt 0 ]]; do
	case "$1" in
	    -c|--cluster) readopt cluster $@ ; shift ; shift ;;
	    --dryrun) dry=1 ; shift ;;
	    --force) force=1 ; shift ;;
	    -s|--show) show=1 ; shift ;;
	    -*|--*=) exit_error "unknown flag $1" ;;
	    *) exit_error "unexpected argument '$1'" ;;
	esac
    done

    validate_cluster_id "$cluster"

    if which jq >/dev/null; then
	local prettyprinter="jq ."
    else
	local prettyprinter="cat"
    fi

    local nodes
    nodes=$(list)

    if [[ $? -ne 0 ]]; then
	exit_error "Failed to get the list of nodes for"\
		   "cluster '$cluster' from AWS"
    fi

    local zkinsts
    zkinsts=$(echo "$nodes" | awk '/^\[zk\]/{print $2"\t"$5"\t"$6}')
    if [[ $? -ne 0 ]]; then
	exit_error "Failed to get the list of Zookeeper hosts from EC2"
    fi

    local zookeeper
    zookeeper=$(echo "$zkinsts" | zk_client_config)
    if [[ $? -ne 0 || -z "$zookeeper" ]]; then
	exit_error "not enough Zookeeper instances are reported up by EC2."\
		   "Expected $(ninst zk) instances, got '$zookeeper'."\
		   "Use lab start-zk to start Zookeeper."
    fi

    local zkhost=$(echo "$zkinsts" | awk '{print $3 ; exit}')

    if [[ -z "$zkhost" ]]; then
	abort "invalid list() output: '$zkinsts'"
    fi

    if [[ -n "$show" ]]; then
	echo "get /conf/${cluster}.conf" | zkcli $zkhost |\
	    grep ",\"cluster\":\"$(basename $cluster)\"," | $prettyprinter
	exit
    fi

    local cluster_nodes
    cluster_nodes=$(echo "$nodes" | grep "^$cluster ")

    if [[ $? -ne 0 ]]; then
	exit_error "No nodes found for cluster '$cluster' in this AWS region."
    fi

    local iids=$(echo "$cluster_nodes" | cut -d ' ' -f 2)

    echo "Checking that nodes are up and running..."

    wait_up $iids

    if [[ -n "$dry" ]]; then
	echo "$cluster_nodes" |\
	    cluster_config $(basename $cluster) $zookeeper| $prettyprinter
	exit
    fi

    if [[ -z "$force" ]]; then
	echo ls /conf/${cluster}.conf | zkcli $zkhost |& tracelog
	if [[ $? -eq 0 ]]; then
	    exit_error "Found an existing config file for cluster '$cluster'"\
		       "in Zookeeper. --show to view, --force to overwrite."
	fi
    fi

    (echo "create /conf"
     echo "create /conf/$(dirname $cluster)"
     echo "create /conf/${cluster}.conf"
     echo -n "set /conf/${cluster}.conf "
     echo "$cluster_nodes" | cluster_config $(basename $cluster) $zookeeper | \
	 tr -d '[:space:]'
     echo) | zkcli $zkhost |& tracelog

    if [[ $? -ne 0 ]]; then
	exit_error "Failed to store config for cluster '$cluster' in "\
		   "Zookeeper. Check $TRACELOG for details."
    fi

    echo "Successfully created a LogDevice config file for cluster '$cluster'"
    echo "and stored it in Zookeeper under /conf/$cluster.conf"
}
