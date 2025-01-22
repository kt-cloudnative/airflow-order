from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CBOE559B',WORKFLOW_ID='3edba4f27b714e1d9bf31278dd62818e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOE559B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3edba4f27b714e1d9bf31278dd62818e')

    cboe559bJob_vol = []
    cboe559bJob_volMnt = []
    cboe559bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cboe559bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cboe559bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboe559bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cboe559bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboe559bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'fbeadb5158964418b1e657a4e71bf4f9',
        'volumes': cboe559bJob_vol,
        'volume_mounts': cboe559bJob_volMnt,
        'env_from':cboe559bJob_env,
        'task_id':'cboe559bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cboe559bJob", "requestDate="+str(datetime.now().strftime("%Y%m%d")),"duplPrvnDate="+str(datetime.now().strftime("%Y%m%d%H%M%S")) ],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3edba4f27b714e1d9bf31278dd62818e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboe559bJob,
        Complete
    ]) 

    # authCheck >> cboe559bJob >> Complete
    workflow








