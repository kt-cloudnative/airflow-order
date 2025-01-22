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
                , WORKFLOW_NAME='PP_CSND225B',WORKFLOW_ID='fff6beba74c34b44b66e8c41b344ba0a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND225B-0.0.prd-tz.4.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 18, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('fff6beba74c34b44b66e8c41b344ba0a')

    csnd225bJob_vol = []
    csnd225bJob_volMnt = []
    csnd225bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnd225bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnd225bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnd225bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnd225bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd225bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '89dc7561909a4567adf1c5c0b5959254',
        'volumes': csnd225bJob_vol,
        'volume_mounts': csnd225bJob_volMnt,
        'env_from':csnd225bJob_env,
        'task_id':'csnd225bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnd225bJob",  "endTranDate=${YYYYMMDD}" , "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('fff6beba74c34b44b66e8c41b344ba0a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd225bJob,
        Complete
    ]) 

    # authCheck >> csnd225bJob >> Complete
    workflow








