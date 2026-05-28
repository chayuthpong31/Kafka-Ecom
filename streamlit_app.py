# Import python packages.
import streamlit as st

# Write directly to the app.
st.title("📈 Ecommerce_Sales")
st.set_page_config(layout="wide")

# Create a database connection to Snowflake.
conn = st.connection("snowflake")
session = conn.session()

df_all = session.sql("""
    SELECT event_date, page_views, add_to_cart, purchases
    FROM KAFKA_DB.STREAMING.EVENT_FUNNEL
""").to_pandas()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.subheader("Page Views by Day")
    st.line_chart(data=df_all, x="EVENT_DATE", y="PAGE_VIEWS")

with col2:
    st.subheader("Add to Carts by Day")
    st.line_chart(data=df_all, x="EVENT_DATE", y="ADD_TO_CART")

with col3:
    st.subheader("Purchases by Day")
    st.line_chart(data=df_all, x="EVENT_DATE", y="PURCHASES")
with col4:
    df = session.sql("""
        SELECT 
            SUM(total_revenue) as total_revenue
        FROM KAFKA_DB.STREAMING.DAILY_CUSTOMER_REVENUE
    """).to_pandas()

    revenue_value = df['TOTAL_REVENUE'].iloc[0]
    formatted_revenue = f"${revenue_value:,.2f}"

    with st.container(border=True):
        
        st.caption("Total Revenue") 
        
        st.markdown(f"""
            <div style="text-align: center; line-height: 20; margin-top: 15px;">
                <h1 style="font-size: 2.5rem; color: white; display: inline-block; vertical-align: middle;">{formatted_revenue}</h1>
            </div>
        """, unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    df = session.sql("""
        SELECT 
            event_date,
            SUM(total_revenue) as total_revenue
        FROM KAFKA_DB.STREAMING.DAILY_CUSTOMER_REVENUE
        GROUP BY event_date
    """).to_pandas()
    
    st.subheader("Revenue by Day")
    st.line_chart(data=df, x="EVENT_DATE", y="TOTAL_REVENUE")

with col2:
    df = session.sql("""
        SELECT 
            event_date,
            SUM(purchase_count) as purchase_count
        FROM KAFKA_DB.STREAMING.DAILY_CUSTOMER_REVENUE
        GROUP BY event_date
    """).to_pandas()
    
    st.subheader("Purchase Count by Day")
    st.line_chart(data=df, x="EVENT_DATE", y="PURCHASE_COUNT")