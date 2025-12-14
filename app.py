from app import create_app

# --------------------------------------------------------------------
#  EXECUÇÃO DIRETA
# --------------------------------------------------------------------
if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, host="0.0.0.0", port=5555)
