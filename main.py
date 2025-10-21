import sys
from .runner import Runner


def main():
	json_path = None
	if len(sys.argv) > 1:
		json_path = sys.argv[1]
	r = Runner(json_path=json_path) if json_path else Runner()
	try:
		r.run()
	except KeyboardInterrupt:
		print("Stopped by user.")
	except Exception as e:
		import traceback
		print("Fatal Error:", e)
		traceback.print_exc()
	finally:
		r.close()


if __name__ == "__main__":
	main()

