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
                , WORKFLOW_NAME='WC_CSNG061B',WORKFLOW_ID='498ebb6f1ebe4dba8943547ce9cb95aa', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSNG061B-0.0.prd-tz.1.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 58, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('498ebb6f1ebe4dba8943547ce9cb95aa')

    csng061bJob_vol = []
    csng061bJob_volMnt = []
    csng061bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng061bJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng061bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng061bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '79cd381678c144d68aed7f4b9d90132d',
        'volumes': csng061bJob_vol,
        'volume_mounts': csng061bJob_volMnt,
        'env_from':csng061bJob_env,
        'task_id':'csng061bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.7',
        'arguments':["--job.name=csng061bJob", "workDate=20241022", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('498ebb6f1ebe4dba8943547ce9cb95aa')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng061bJob,
        Complete
    ]) 

    # authCheck >> csng061bJob >> Complete
    workflow








