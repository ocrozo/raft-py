import httplib
import xmlrpclib


def get_leader(nodes):
    leader = None
    for n in nodes:
        try:
            if n.is_leader():
                leader = n
                break
        except httplib.HTTPException:
            print ('HTTPException')
        except Exception:
            print ('Exception')
    return leader


def add_entry(nodes, tid, data):
    committed = False
    while not committed:
        try:
            leader = get_leader(nodes)
            if leader is not None:
                committed = leader.addEntry(tid, data)
        except httplib.HTTPException:
            print ('HTTPException')
        except Exception:
            print ('Exception')


def main():
    tid = 0    # unique transaction id
    nodes = []
    node_ids = ["134.214.202.220","134.214.202.221","134.214.202.222"]

    for nodeId in node_ids:
        node = xmlrpclib.Server("http://"+nodeId+":8000", allow_none=True)
        nodes.append(node)

    add_entry(nodes, tid, "test")
    # tid += 1
    # addEntry(nodes, tid, "abcd")

    print ('Client Done')


if __name__ == "__main__":
    main()
